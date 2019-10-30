package com.caselchen.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaStartPositionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
//        kafkaProps.setProperty("group.id", "test");
//        kafkaProps.setProperty("zookeeper.connect", "localhost:2181");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("number", new SimpleStringSchema(), kafkaProps);
//        consumer.setStartFromTimestamp(System.currentTimeMillis() - 1000 * 60 * 5);
//        consumer.setStartFromEarliest();
//        consumer.setStartFromLatest();
        consumer.setStartFromGroupOffsets();

        DataStream<String> stream = env.addSource(consumer);
        stream.print();

        env.execute("KafkaStartPositionTest");

    }
}
