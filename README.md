# playing-with-kafka-streams

Please run zookeeper, kafka and schema-registry on default ports. Then create 2 topics: avro-users-with-ts and window-key-count. Afterwards, use sbt's run task and choose AvroStreams. Open new sbt session and run AvroProducer. After a while, check records in window-key-count topic. You should see that even if TimeWindow is set to be closed after 15 second, it takes 3 minutes to stop processing records with old timestamp.
