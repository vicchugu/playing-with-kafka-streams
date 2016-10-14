package test

import java.util.{Properties, UUID}

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}

object IntegerStream {
  def main(args: Array[String]): Unit = {
    val builder = new KStreamBuilder

    val users: KStream[String, Integer] = builder.stream("fibonacci-topic")

    users.foreach((k,v) => println(s"key is : $k, value is $v"))

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("schema.registry.url", "http://localhost:8081")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("client.id", UUID.randomUUID().toString)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-aggregation-scala-example")
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass.getName)

    val stream = new KafkaStreams(builder, props)
    stream.start()
  }
}
