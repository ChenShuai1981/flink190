package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * 时间窗口内求TopN示例
 */
public class WindowTopNExample {

    private static final int N = 3;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String[] lines = new String[]{
                "orderId02,1573483405000,gdsId01,500,beijing",
                "orderId03,1573483408000,gdsId02,200,beijing",
                "orderId03,1573483408000,gdsId03,300,beijing",
                "orderId03,1573483408000,gdsId04,400,beijing",
                "orderId07,1573483600000,gdsId01,600,beijing",
//                "orderId07,1573483610000,gdsId02,500,beijing",
//                "orderId08,1573484200000,gdsId01,400,beijing",
//                "orderId08,1573484500000,gdsId02,300,beijing"
        };

        DataStream<Order> orderStream = env.fromElements(lines).map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] parts = value.split(",");
                return new Order(parts[0], Long.valueOf(parts[1]), parts[2], Double.valueOf(parts[3]), parts[4]);
            }
        }).assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(30)) {
                @Override
                public long extractTimestamp(Order element) {
                    return element.getOrderTime();
                }
            });

        /**
         * 先按地区和商品编号聚合求出每10分钟的销售总金额
         * (areaId1, gdsId1) -> amount11
         * (areaId1, gdsId2) -> amount12
         * (areaId2, gdsId1) -> amount21
         * (areaId2, gdsId2) -> amount22
         */
        DataStream<Order> amountStream = orderStream
                .keyBy((KeySelector<Order, String>) value -> value.getAreaId() + "_" + value.getGdsId())
                .timeWindow(Time.minutes(10))
                .reduce((ReduceFunction<Order>) (value1, value2) ->
                        new Order(value1.orderId, value1.orderTime, value1.gdsId, value1.amount + value2.amount, value1.areaId));

        /**
         * 再按地区聚合求出每10分钟的所有不同商品销售总金额TopN
         *
         * areaId1:
         *  top1. gdsId1, amount11
         *  top2. gdsId2, amount12
         *  top3. gdsId3, amount13
         *
         *  areaId2:
         *  top1. gdsId1, amount21
         *  top2. gdsId2, amount22
         *  top3. gdsId3, amount23
         *
         */
        amountStream.keyBy(new KeySelector<Order, String>() {
            @Override
            public String getKey(Order value) throws Exception {
                return value.getAreaId();
            }
        }).timeWindow(Time.minutes(10)).apply(new WindowFunction<Order, Order, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Order> input, Collector<Order> out) throws Exception {
                System.out.println("==area=== " + key);
                TreeSet<Order> topMap = new TreeSet(new Comparator<Order>() {

                    @Override
                    public int compare(Order o1, Order o2) {
                        return Double.compare(o2.amount, o1.amount);
                    }
                });
                input.forEach(x -> {
                    if (topMap.size() >= N) {
                        Order min = topMap.first();
                        if(x.amount > min.amount) {
                            topMap.pollFirst(); //舍弃
                            topMap.add(x);
                        }
                    } else {
                        topMap.add(x);
                    }
                });

                //这里直接做打印
                topMap.forEach(x -> {
                    System.out.println(x);
                });
            }
        });

        env.execute("WinTopNExample");
    }

    @Data
    @AllArgsConstructor
    static class Order implements Serializable {
        private String orderId;
        private Long orderTime;
        private String gdsId;
        private Double amount;
        private String areaId;
    }
}
