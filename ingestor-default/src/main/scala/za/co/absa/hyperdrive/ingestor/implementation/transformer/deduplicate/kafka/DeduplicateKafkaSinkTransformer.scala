/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka

import java.time.Duration
import java.util
import java.util.{Collections, Properties, UUID}

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.commons.configuration2.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformer, StreamTransformerFactory}
import za.co.absa.hyperdrive.ingestor.api.utils.ConfigUtils.{getOrThrow, getPropertySubset}
import za.co.absa.hyperdrive.ingestor.api.utils.StreamWriterUtil
import za.co.absa.hyperdrive.ingestor.api.writer.StreamWriterCommonAttributes
import za.co.absa.hyperdrive.ingestor.implementation.reader.kafka.KafkaStreamReader
import za.co.absa.hyperdrive.ingestor.implementation.transformer.avro.confluent.{ConfluentAvroDecodingTransformer, ConfluentAvroEncodingTransformer}
import za.co.absa.hyperdrive.ingestor.implementation.transformer.deduplicate.kafka.kafka010.KafkaSourceOffset
import za.co.absa.hyperdrive.ingestor.implementation.writer.kafka.KafkaStreamWriter

import scala.collection.JavaConverters._
import scala.collection.mutable


private[transformer] class DeduplicateKafkaSinkTransformer(
  val readerSchemaRegistryUrl: String,
  val readerTopic: String,
  val readerBrokers: String,
  val readerExtraOptions: Map[String, String],
  val writerSchemaRegistryUrl: String,
  val writerTopic: String,
  val writerBrokers: String,
  val writerExtraOptions: Map[String, String],
  val checkpointLocation: String,
  val idColumn: String) extends StreamTransformer {
  private val logger = LogManager.getLogger
  private val timeout = Duration.ofSeconds(5L)

  override def transform(dataFrame: DataFrame): DataFrame = {
    val spark = dataFrame.sparkSession
    val offsetLog = new OffsetSeqLog(spark, new Path(checkpointLocation, "offsets").toString)
    val commitLog = new CommitLog(spark, new Path(checkpointLocation, "commits").toString)
    val latestOffsetLog = offsetLog.getLatest().map(_._1)
    val latestCommitLog = commitLog.getLatest().map(_._1)

    if (latestOffsetLog != latestCommitLog) {
      deduplicateDataFrame(dataFrame, offsetLog, commitLog)
    } else {
      dataFrame
    }
  }

  private def deduplicateDataFrame(dataFrame: DataFrame, offsetLog: OffsetSeqLog, commitLog: CommitLog) = {
    logger.info("Deduplicate rows after retry")
    val sourceConsumer = createConsumer(readerBrokers, readerExtraOptions, readerSchemaRegistryUrl)
    seekToOffsets(sourceConsumer, offsetLog, commitLog)

    val sourceRecordsCount = try {
      getAllAvailableMessagesCount(sourceConsumer)
    } catch {
      case throwable: Throwable => logger.error(s"An unexpected error occurred while consuming from topic $readerTopic", throwable)
        throw throwable
    } finally {
      sourceConsumer.close()
    }

    val sinkConsumer = createConsumer(writerBrokers, writerExtraOptions, writerSchemaRegistryUrl)
    val sinkTopicPartitions = getTopicPartitions(sinkConsumer, writerTopic)
    val latestSinkRecords = try {
      sinkTopicPartitions.map(topicPartition => {
        getAtLeastNLatestRecords(sinkConsumer, topicPartition, sourceRecordsCount)
      })
    } catch {
      case throwable: Throwable => logger.error(s"An unexpected error occurred while consuming from topic $writerTopic", throwable)
        throw throwable
    } finally {
      sinkConsumer.close()
    }

    val publishedIds = latestSinkRecords.flatten.map(record => {
      try {
        record.value().get(idColumn).asInstanceOf[Utf8].toString
      } catch {
        case throwable: Throwable => logger.error(s"Could not get $idColumn from record, schema is ${record.value().getSchema}", throwable)
          throw throwable
      }
    })
    import org.apache.spark.sql.functions._
    dataFrame.filter(not(col(idColumn).isInCollection(publishedIds)))
  }

  private def seekToOffsets(consumer: KafkaConsumer[GenericRecord, GenericRecord], offsetLog: OffsetSeqLog, commitLog: CommitLog) = {
    val sourceTopicPartitionOffsetsOpt = getTopicPartitionsFromOffsets(offsetLog, commitLog)
    consumer.subscribe(Collections.singletonList(readerTopic), new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        sourceTopicPartitionOffsetsOpt match {
          case Some(topicPartitionOffsets) => topicPartitionOffsets.foreach {
            case (topicPartition, offset) => consumer.seek(topicPartition, offset)
          }
          case None =>
            val partitions = getTopicPartitions(consumer, readerTopic)
            consumer.seekToBeginning(partitions.asJava)
        }
      }
    })
  }

  private def getTopicPartitions(consumer: KafkaConsumer[GenericRecord, GenericRecord], topic: String) = {
    consumer.partitionsFor(topic).asScala.map(p => new TopicPartition(p.topic(), p.partition()))
  }

  private def getAtLeastNLatestRecords(consumer: KafkaConsumer[GenericRecord, GenericRecord], topicPartition: TopicPartition, numberOfRecords: Int) = {
    consumer.assign(Seq(topicPartition).asJava)
    val endOffsets = consumer.endOffsets(Seq(topicPartition).asJava).asScala
    if (endOffsets.size != 1) {
      throw new IllegalStateException(s"Expected exactly 1 end offset, got ${endOffsets}")
    }
    val partition = endOffsets.keys.head
    val offset = endOffsets.values.head

    var records: Seq[ConsumerRecord[GenericRecord, GenericRecord]] = Seq()
    var offsetLowerBound = offset
    while(records.size < numberOfRecords && offsetLowerBound != 0) {
      offsetLowerBound = Math.max(0, offsetLowerBound - numberOfRecords)
      consumer.seek(partition, offsetLowerBound)
      records = getAllAvailableMessages(consumer)
    }

    records
  }

  private def getAllAvailableMessagesCount(consumer: KafkaConsumer[GenericRecord, GenericRecord]): Int = {
    import scala.util.control.Breaks._
    var recordsCount = 0
    breakable {
      while (true) {
        val currentRecordsCount = consumer.poll(timeout).count()
        if (currentRecordsCount == 0) {
          break()
        }
        recordsCount += currentRecordsCount
      }
    }
    recordsCount
  }

  private def getAllAvailableMessages(consumer: KafkaConsumer[GenericRecord, GenericRecord]) = {
    import scala.util.control.Breaks._
    var records: Seq[ConsumerRecord[GenericRecord, GenericRecord]] = mutable.Seq()
    breakable {
      while (true) {
        val newRecords = consumer.poll(timeout).asScala.toSeq
        if (newRecords.isEmpty) {
          break()
        }
        records ++= newRecords
      }
    }
    records
  }

  private def createConsumer(brokers: String, extraOptions: Map[String, String], schemaRegistryUrl: String) = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, s"hyperdrive_consumer_${UUID.randomUUID().toString}")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"hyperdrive_group_${UUID.randomUUID().toString}")
    extraOptions.foreach {
      case (key, value) => props.put(key, value)
    }
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    new KafkaConsumer[GenericRecord, GenericRecord](props)
  }

  def getTopicPartitionsFromOffsets(offsetLog: OffsetSeqLog, commitLog: CommitLog): Option[Map[TopicPartition, Long]] = {
    val offsetSeqOpt = commitLog.getLatest().map(_._1)
      .flatMap(batchId => offsetLog.get(batchId))
      .map(offsetLog => offsetLog.offsets)
    val result = if (offsetSeqOpt.isDefined) {
      if (offsetSeqOpt.get.size == 1) {
        if (offsetSeqOpt.get.head.isDefined) {
          Some(KafkaSourceOffset.getPartitionOffsets(offsetSeqOpt.get.head.get))
        } else {
          throw new IllegalStateException("Offset must be defined, got None")
        }
      } else {
         throw new IllegalStateException(s"Cannot support more than 1 source, got ${offsetSeqOpt.toString}")
      }
    } else {
      None
    }
    result
  }
}

object DeduplicateKafkaSinkTransformer extends StreamTransformerFactory with DeduplicateKafkaSinkTransformerAttributes {

  override def apply(config: Configuration): StreamTransformer = {
    val readerSchemaRegistryUrl = config.getString(schemaRegistryUrl)
    val readerTopic = config.getString(KafkaStreamReader.KEY_TOPIC)
    val readerBrokers = config.getString(KafkaStreamReader.KEY_BROKERS)
    val readerExtraOptions = getPropertySubset(config, KafkaStreamReader.getExtraConfigurationPrefix.get)

    val writerSchemaRegistryUrl = config.getString(schemaRegistryUrl)
    val writerTopic = config.getString(KafkaStreamWriter.KEY_TOPIC)
    val writerBrokers = config.getString(KafkaStreamWriter.KEY_BROKERS)
    val writerExtraOptions = getPropertySubset(config, KafkaStreamWriter.optionalConfKey)

    val checkpointLocation = StreamWriterUtil.getCheckpointLocation(config)

    val idColumn = getOrThrow(IdColumn, config, errorMessage = s"Destination directory not found. Is '${IdColumn}' defined?")
    new DeduplicateKafkaSinkTransformer(readerSchemaRegistryUrl, readerTopic, readerBrokers, readerExtraOptions,
      writerSchemaRegistryUrl, writerTopic, writerBrokers, writerExtraOptions,
      checkpointLocation, idColumn)
  }

  override def getMappingFromRetainedGlobalConfigToLocalConfig(globalConfig: Configuration): Map[String, String] = {
//    TODO: What about subsets?
    Set(
      KafkaStreamReader.KEY_TOPIC,
      KafkaStreamReader.KEY_BROKERS,
      KafkaStreamWriter.KEY_TOPIC,
      KafkaStreamWriter.KEY_BROKERS,
      StreamWriterCommonAttributes.keyCheckpointBaseLocation
    ).map(e => e -> e).toMap
  }
}


