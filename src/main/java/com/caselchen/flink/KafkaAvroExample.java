package com.caselchen.flink;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaAvroExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<Student> myConsumer = new FlinkKafkaConsumer<>(
                "student-avro",
                AvroDeserializationSchema.forSpecific(Student.class),
                properties);

        DataStream<Student> stream = env.addSource(myConsumer);
        stream.print();

        env.execute("KafkaAvroExample");
    }

}
