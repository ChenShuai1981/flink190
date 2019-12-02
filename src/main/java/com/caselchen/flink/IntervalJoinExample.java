package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Order> orderStream = env.fromElements(
                new Order("10000", Timestamp.valueOf("2017-12-18 15:00:10"), 2000L),
                new Order("10001", Timestamp.valueOf("2017-12-18 15:00:15"), 4000L),
                new Order("10002", Timestamp.valueOf("2017-12-18 15:00:20"), 3000L),
                new Order("10003", Timestamp.valueOf("2017-12-18 15:00:20"), 3000L),
                new Order("10004", Timestamp.valueOf("2017-12-18 15:00:20"), 4000L),
                new Order("10005", Timestamp.valueOf("2017-12-18 15:00:20"), 1000L),
                new Order("10006", Timestamp.valueOf("2017-12-18 15:00:30"), 1000L),
                new Order("10007", Timestamp.valueOf("2017-12-18 15:00:30"), 5000L),
                new Order("10008", Timestamp.valueOf("2017-12-18 15:00:40"), 6000L),
                new Order("10009", Timestamp.valueOf("2017-12-18 15:00:50"), 8000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.getOrderTime().getTime();
            }
        });

        DataStream<Shipment> shipmentStream = env.fromElements(
                new Shipment("1", "10000", Timestamp.valueOf("2017-12-18 16:00:10"), 2000L),
                new Shipment("2", "10001", Timestamp.valueOf("2017-12-18 17:00:15"), 4000L),
                new Shipment("3", "10002", Timestamp.valueOf("2017-12-18 18:00:20"), 3000L),
                new Shipment("4", "10003", Timestamp.valueOf("2017-12-18 19:00:21"), 3000L),
                new Shipment("5", "10004", Timestamp.valueOf("2017-12-18 15:00:20"), 4000L),
                new Shipment("6", "10005", Timestamp.valueOf("2017-12-18 16:00:20"), 1000L),
                new Shipment("7", "10006", Timestamp.valueOf("2017-12-18 17:00:30"), 1000L),
                new Shipment("8", "10007", Timestamp.valueOf("2017-12-18 18:00:30"), 5000L),
                new Shipment("9", "10008", Timestamp.valueOf("2017-12-18 14:00:41"), 6000L),
                new Shipment("10", "10009", Timestamp.valueOf("2017-12-18 11:00:49"), 8000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Shipment>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Shipment element) {
                return element.getShipTime().getTime();
            }
        });

        // 运单在订单产生后的4小时内有效
        shipmentStream.keyBy((KeySelector<Shipment, String>) shipment -> shipment.getOrderId())
                .intervalJoin(orderStream.keyBy((KeySelector<Order, String>) order -> order.getId()))
                .between(Time.hours(-4), Time.hours(0))
                .process(new ProcessJoinFunction<Shipment, Order, OrderShipment>(){
                    @Override
                    public void processElement(Shipment shipment, Order order, Context ctx, Collector<OrderShipment> out) throws Exception {
                        String orderId = order.getId();
                        String shipmentId = shipment.getId();
                        Timestamp orderTime = order.getOrderTime();
                        Timestamp shipTime = shipment.getShipTime();
                        long amount = order.getAmount();
                        long weight = shipment.getWeight();
                        OrderShipment orderShipment = new OrderShipment(orderId, shipmentId, orderTime, shipTime, amount, weight);
                        out.collect(orderShipment);
                    }
                }).print();

        env.execute("IntervalJoinExample");
    }

    @Data
    @AllArgsConstructor
    static class Order {
        private String id;
        private Timestamp orderTime;
        private long amount;
    }

    @Data
    @AllArgsConstructor
    static class Shipment {
        private String id;
        private String orderId;
        private Timestamp shipTime;
        private long weight;
    }

    @Data
    @AllArgsConstructor
    static class OrderShipment {
        private String orderId;
        private String shipmentId;
        private Timestamp orderTime;
        private Timestamp shipTime;
        private long amount;
        private long weight;
    }
}
