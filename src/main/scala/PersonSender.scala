package org.apache.flink.table.sinks

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.caselchen.flink.Person
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object PersonSender extends App {
  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  val producer = new KafkaProducer[String, String](props)
  val topic = "input_topic"

  try {
    val age = Random.nextInt(50)
    val person = new Person("name"+age, age)
    val json = JSON.toJSONString(person, SerializerFeature.WriteMapNullValue)
    val record = new ProducerRecord[String, String](topic, json)
    val metadata = producer.send(record)
    printf(s"sent record(key=%s value=%s) " +
      "meta(partition=%d, offset=%d)\n",
      record.key(), record.value(),
      metadata.get().partition(),
      metadata.get().offset())
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }

}
