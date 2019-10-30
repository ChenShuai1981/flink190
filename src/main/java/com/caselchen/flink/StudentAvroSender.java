package com.caselchen.flink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.Random;

public class StudentAvroSender {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
        // Hard coding topic too.
        String topic = "student-avro";

        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        SpecificDatumWriter writer = new SpecificDatumWriter(Student.class);
        for (int i=0; i<10; i++) {
            Random rnd = new Random();
            int age = rnd.nextInt(30);
            Student student = new Student();
            student.setAge(age);
            student.setName("StudentName" + age);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(student, encoder);
            encoder.flush();
            out.close();
            byte[] serializedBytes = out.toByteArray();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, student.getName(), serializedBytes);
            producer.send(record).get();
        }
        producer.close();
    }
}
