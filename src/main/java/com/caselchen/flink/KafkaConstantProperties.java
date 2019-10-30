package com.caselchen.flink;

public class KafkaConstantProperties {

    /**
     * kafka broker 地址
     */
    public static final String KAFKA_BROKER = "localhost:9092";

    /**
     * zk 地址，低版本 kafka 使用，高版本已丢弃
     */
    public static final String ZOOKEEPER_HOST = "localhost:2181";

    /**
     * flink 计算使用topic 1
     */
    public static final String FLINK_COMPUTE_TOPIC_IN1 = "mastertest";

    /**
     * flink消费结果，输出到kafka, topic 数据
     */
    public static final String FLINK_DATA_SINK_TOPIC_OUT1 = "flink_compute_result_out1";

}