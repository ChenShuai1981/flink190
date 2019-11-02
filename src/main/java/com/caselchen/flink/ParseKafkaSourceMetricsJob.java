package com.caselchen.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class ParseKafkaSourceMetricsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test");

        CustomerKafkaConsumer<RawData> consumer = new CustomerKafkaConsumer("flink-metrics", new ParseDeserialization(), properties);
        env.addSource(consumer).print();

        env.execute("ParseKafkaSourceMetricsJob");
    }
}
