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

package za.co.absa.hyperdrive.driver.kafkaexactlyonce

import java.util.Properties

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers}

class LocalhostKafkaTestData extends FlatSpec with Matchers {

  def createProducer(): KafkaProducer[Int, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092,http://localhost:9091")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaToKafkaProducer")
    props.put("schema.registry.url", "http://localhost:8081")
    new KafkaProducer[Int, GenericRecord](props)
  }

  behavior of "LocalhostKafkaTestData"

  it should "insert test data into a topic" in {
    // given

    val sourceTopic = "testdata"
    val numberOfRecords = 50
    val schemaString = raw"""{"type": "record", "name": "$sourceTopic", "fields": [
      {"type": "string", "name": "field1"},
      {"type": "int", "name": "field2"}
      ]}"""
    val schema = new Parser().parse(schemaString)

    val producer = createProducer()
    for (i <- 0 until numberOfRecords) {
      val record = new GenericData.Record(schema)
      record.put("field1", "hello")
      record.put("field2", i)
      val producerRecord = new ProducerRecord[Int, GenericRecord](sourceTopic, 1, record)
      producer.send(producerRecord)
    }
    producer.flush()

  }
}
