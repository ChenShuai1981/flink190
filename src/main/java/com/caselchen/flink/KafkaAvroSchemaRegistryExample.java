package com.caselchen.flink;

import com.example.Sensor;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaAvroSchemaRegistryExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "sensor");

        FlinkKafkaConsumer<Sensor> myConsumer = new FlinkKafkaConsumer<>(
                "sensor-topic",
                ConfluentRegistryAvroDeserializationSchema.forSpecific(Sensor.class, "http://localhost:8081"),
                properties);

        DataStream<Sensor> stream = env.addSource(myConsumer);
        stream.print();

        env.execute("KafkaAvroSchemaRegistryExample");
    }
}
