package org.apache.flink.table.sinks

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object RuleSender extends App {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  val producer = new KafkaProducer[String, String](props)
  val topic = "rule_topic"

  try {
    val rule =
      """
        |package rules
        |
        |import com.caselchen.flink.Person;
        |
        |rule "rule1"
        |    when
        |       b:Person( age >= 13 );
        |    then
        |        b.setResult(b.getAge() + " is above 13 age");
        |end
        |
        |rule "rule2"
        |    when
        |       b:Person( age < 13 );
        |    then
        |        b.setResult(b.getAge() + " is under 13 age");
        |end
      """.stripMargin
    val record = new ProducerRecord[String, String](topic, rule)
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
