package test

import java.util.{Properties, UUID}
import java.lang.{Long => JLong}
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, TimeWindows}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.TimestampExtractor

case class User(sum: Integer, name: String, dateTime: LocalDateTime)

object AvroStreams {
  def main(args: Array[String]): Unit = {
    val builder = new KStreamBuilder

    val users: KStream[String, GenericRecord] = builder.stream("avro-users-with-ts")

    val stringSerde = Serdes.String()

    val valuesMapped: KStream[String, User] = users.mapValues(createUser)
    val names: KStream[String, Integer] = valuesMapped.map { (_, v) => new KeyValue(v.name, v.sum) }
    val timeWindow =
      TimeWindows.of("GeoPageViewsWindow", 5 * 1000L)
        .advanceBy(5 * 1000L)
        .until(15 * 1000L)

    val counted = names.countByKey(timeWindow)
    val str: KStream[String, String] = counted.toStream((k, _) => k.window.start.toString)
      .mapValues(_.toString)

    str.to(stringSerde, stringSerde, "window-key-count")

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("schema.registry.url", "http://localhost:8081")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("client.id", UUID.randomUUID().toString)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-windowing-scala-example")
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
    props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[EventTimestampExtractor])

    val stream = new KafkaStreams(builder, props)
    stream.start()
  }

  def createUser(r: GenericRecord): User = {
    val sum = r.get("sum").asInstanceOf[Integer]
    val name = r.get("name").toString
    val ts = LocalDateTime.ofInstant(Instant.ofEpochMilli(r.get("ts").asInstanceOf[JLong]), ZoneId.systemDefault())
    User(sum, name, ts)
  }
}

class EventTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef]): Long = {
    record.value
      .asInstanceOf[GenericRecord]
      .get("ts")
      .asInstanceOf[JLong]
  }
}
