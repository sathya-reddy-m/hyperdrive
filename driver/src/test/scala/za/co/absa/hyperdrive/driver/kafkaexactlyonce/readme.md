1. Start local confluent instance. Assumed schema-registry at 8081, kafka brokers 9091, 9092. Otherwise change in LocalhostKafkaTestData and Ingestion.properties
2. Executing LocalhostKafkaTestData creates testdata
3. ./exec.sh creates checkpoint-location and executes kafka-to-kafka ingestion from topic testdata to testdata_sink
4. There will be 50 records in testdata_sink

5. Remove checkpoint-directory to simulate application crash just before committing the microbatch
6. ./exec.sh again
7. There will be 100 records in testdata_sink. Wanted: Only 50, no duplicates.

