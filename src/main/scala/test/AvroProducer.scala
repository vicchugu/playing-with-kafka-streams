package test

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.util.{Properties, UUID}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object AvroProducer {
  def main(args: Array[String]): Unit = {
    val date = LocalDate.now()
    val producer = new KafkaProducer[String, GenericRecord](CommonProperties())

    while (true) {
      //        Change value here when you test it.
      val dt1 = LocalDateTime.of(date, LocalTime.of(8, 36))
      producer.send(Record.create(User(1, "Odersky", dt1)))
      Thread.sleep(1000L)

      val dt2 = LocalDateTime.of(date, LocalTime.now())
      producer.send(Record.create(User(2, "Odersky", dt2)))
      Thread.sleep(1000L)
    }
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
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("client.id", UUID.randomUUID().toString)
    props
  }
}

object Record {
  val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/schema.avsc")).mkString)
  val topic = "avro-users-with-ts"

  def create(user: User): ProducerRecord[String, GenericRecord] = {
    val genericUser: GenericRecord = new GenericData.Record(schema)
    genericUser.put("sum", user.sum)
    genericUser.put("name", user.name)
    genericUser.put("ts", user.dateTime.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli)
    new ProducerRecord[String, GenericRecord](topic, genericUser)
  }
}