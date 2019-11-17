package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

import java.io.Serializable;

public class ContinuousEventTimeTriggerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);

//        Properties kafkaConfig = new Properties();
//        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
//
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("topic1", new SimpleStringSchema(), kafkaConfig);
//        DataStream<String> stringStream = env.addSource(consumer);

        DataStream<String> stringStream = env.fromElements("orderId03,1573963096000,gdsId03,300,beijing", "orderId03,1573963156000,gdsId03,200,hangzhou");

        stringStream.map((MapFunction<String, Order>) value -> {
            String[] parts = value.split(",");
            return new Order(parts[0], Long.valueOf(parts[1]), parts[2], Double.valueOf(parts[3]), parts[4]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(30)) {

            @Override
            public long extractTimestamp(Order order) {
                return order.getOrderTime();
            }
        }).map((MapFunction<Order, AreaOrder>) order -> new AreaOrder(order.getAreaId(), order.getAmount())).keyBy(new KeySelector<AreaOrder, String>() {
            @Override
            public String getKey(AreaOrder value) throws Exception {
                return value.getAreaId();
            }
        })
        .timeWindow(Time.hours(1))
        .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))
        .reduce((ReduceFunction<AreaOrder>) (value1, value2) -> new AreaOrder(value1.areaId, value1.amount + value2.amount))
        .print();

        env.execute("ContinuousEventTimeTriggerDemo");
    }

    @Data
    @AllArgsConstructor
    @ToString
    private static class Order implements Serializable {
        private String orderId;
        private Long orderTime;
        private String gdsId;
        private Double amount;
        private String areaId;
    }

    @Data
    @AllArgsConstructor
    @ToString
    private static class AreaOrder implements Serializable {
        private String areaId;
        private Double amount;
    }
}
