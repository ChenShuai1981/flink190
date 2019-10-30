package com.caselchen.flink;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSchemaRegistryJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "test");
        kafkaProps.setProperty("zookeeper.connect", "localhost:2181");

        String schemaRegistryUrl = "http://localhost:8081";

        FlinkKafkaConsumer<Student> consumer = new FlinkKafkaConsumer("student", ConfluentRegistryAvroDeserializationSchema.forSpecific(Student.class, schemaRegistryUrl), kafkaProps);
        DataStreamSource<Student> input = env.addSource(consumer);
//        input.print();
        SingleOutputStreamOperator<String> mapToString = input
                .map((MapFunction<Student, String>) SpecificRecordBase::toString);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "student2",
                new SimpleStringSchema(),
                kafkaProps);

        mapToString.addSink(producer);
        env.execute("KafkaSchemaRegistryJob");

    }
}
