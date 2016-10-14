# playing-with-kafka-streams

Please run zookeeper, kafka and schema-registry on default ports. Then create 3 topics: avro-users, avro-temp, avro-aggregate. Afterwards, use sbt's run task and choose AvroStreams. Open new sbt session and run AvroProducer. After a while, check output of first task. You should see error message.
