package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class IntervalJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        FlinkKafkaConsumer orderConsumer = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), kafkaConfig);
        FlinkKafkaConsumer addressConsumer = new FlinkKafkaConsumer<>("topic2", new SimpleStringSchema(), kafkaConfig);

        KeyedStream<Order, String> orderStream = env.addSource(orderConsumer).map((MapFunction<String, Order>) value -> {
            String[] a = value.split(",");
            Order order = new Order(a[0], a[1], a[2], Double.valueOf(a[3]), a[4], Long.valueOf(a[5]));
            return order;
        }).keyBy("addrId");

        KeyedStream<Address, String> addressStream = env.addSource(addressConsumer).map((MapFunction<String, Address>) value -> {
            String[] a = value.split(",");
            Address address = new Address(a[0], a[1], a[2], Long.valueOf(a[3]));
            return address;
        }).keyBy("addrId");

        orderStream.intervalJoin(addressStream)
                .between(Time.seconds(1), Time.seconds(5))
                .process(new ProcessJoinFunction<Order, Address, RsInfo>() {

                    @Override
                    public void processElement(Order left, Address right, Context ctx, Collector<RsInfo> out) throws Exception {
                        System.out.println("==在这里得到相同key的两条数据===");
                        System.out.println("left: " + left);
                        System.out.println("right: " + right);
                    }
                });

        env.execute("IntervalJoinDemo");

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    private static class Order {
        private String orderId;
        private String userId;
        private String gdsId;
        private Double amount;
        private String addrId;
        private Long time;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    private static class Address {
        private String orderId;
        private String userId;
        private String address;
        private Long time;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    private static class RsInfo {
        private String orderId;
        private String userId;
        private String gdsId;
        private Double amount;
        private String addrId;
        private String address;
    }
}
