package test

import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import kafka.message.DefaultCompressionCodec
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object IntegerProducer {
  def main(args: Array[String]): Unit = {
    val topic = "fibonacci-topic"

    val props = new Properties()
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[IntegerSerializer])
    props.put("bootstrap.servers", "localhost:9092")
    props.put("compression.codec", DefaultCompressionCodec.codec.toString)
    props.put("producer.type", "async")
    props.put("batch.num.messages", "200")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")
    props.put("client.id", UUID.randomUUID().toString)

    val producer = new KafkaProducer[String, Integer](props)

    val nums = List(1, 1, 2, 3, 5, 8, 13)
    nums.foreach { n =>
      producer.send(new ProducerRecord[String, Integer](topic, n))
        .get(10, TimeUnit.SECONDS)
    }

    println(s" ${nums.length} records inserted.")
  }
}