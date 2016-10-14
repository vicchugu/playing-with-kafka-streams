name := "kafka-streams-playground"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-Xexperimental", "-feature")

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.10.0.1",
  "org.apache.kafka" % "kafka-streams" % "0.10.0.1",
  "org.apache.avro"  %  "avro"  %  "1.8.1",
  "io.confluent" % "kafka-avro-serializer" % "3.0.1"
)

resolvers += "Confluent repos" at "http://packages.confluent.io/maven/"