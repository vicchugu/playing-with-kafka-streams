package test

import java.util.{Properties, UUID}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object AvroProducer {
  def main(args: Array[String]): Unit = {
    val users = List(User(1, "Martin Odersky"), User(2, "Erik Meijer"), User(3, "Martin Odersky"))
    val producer = new KafkaProducer[String, GenericRecord](CommonProperties())

    users.map(Record.create)
      .foreach(rec => producer.send(rec))

    producer.close()
  }
}

object CommonProperties {
  def apply(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://localhost:8081")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("client.id", UUID.randomUUID().toString)
    props
  }
}

object Record {
  val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/schema.avsc")).mkString)
  val topic = "avro-users"

  def create(user: User): ProducerRecord[String, GenericRecord] = {
    val genericUser: GenericRecord = new GenericData.Record(schema)
    genericUser.put("sum", user.sum)
    genericUser.put("name", user.name)
    new ProducerRecord[String, GenericRecord](topic, genericUser)
  }
}