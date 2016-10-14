package test

import java.util.{Properties, UUID}

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import KeyValueImplicits._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes

case class User(sum: Integer, name: String)

object AvroStreams {
  def main(args: Array[String]): Unit = {
    val builder = new KStreamBuilder

    val users: KStream[String, GenericRecord] = builder.stream("avro-users")

    val stringSerde = Serdes.String()
    val intSerde = Serdes.Integer()

    val valuesMapped: KStream[String, User] = users.mapValues(createUser)
    val names: KStream[String, Integer] = valuesMapped.map { (_, v) => (v.name, v.sum)}
    val temp = names.through(stringSerde, intSerde, "avro-temp")

    val agg: KTable[String, Integer] = temp.reduceByKey((s1, s2) => s1 + s2, "Aggregate")
    agg.foreach((k,v) => println(s"key is : $k, value is $v"))

    agg.to(stringSerde, intSerde, "avro-aggregate")

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
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])

    val stream = new KafkaStreams(builder, props)
    stream.start()
  }

  def createUser(r: GenericRecord): User = {
    val sum = r.get("sum").asInstanceOf[Integer]
    val name = r.get("name").toString
    User(sum, name)
  }
}
