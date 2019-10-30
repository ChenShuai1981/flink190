package com.caselchen.flink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class StudentSender {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", KafkaAvroSerializer.class.getCanonicalName());
        props.put("schema.registry.url", "http://localhost:8081");
        // Hard coding topic too.
        String topic = "student";

        Producer<String, Student> producer = new KafkaProducer<String, Student>(props);

        for (int i=0; i<10; i++) {
            Random rnd = new Random();
            int age = rnd.nextInt(30);
            Student student = new Student();
            student.setAge(age);
            student.setName("StudentName" + age);
            ProducerRecord<String, Student> record = new ProducerRecord<String, Student>(topic, student.getName(), student);
            producer.send(record).get();
        }
        producer.close();
    }
}
