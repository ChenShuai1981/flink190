package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class GlobalTopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(5000);
        env.setParallelism(1);

//        Properties kafkaConfig = new Properties();
//        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
//        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
//
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("topic1", new SimpleStringSchema(), kafkaConfig);
//        DataStream<String> stringStream = env.addSource(consumer);

        DataStream<String> stringStream = env.fromCollection(Arrays.asList(
                "orderId01,1573874530000,gdsId03,300,beijing",
                "orderId02,1573874540000,gdsId01,100,beijing",
                "orderId02,1573874540000,gdsId04,200,beijing",
                "orderId02,1573874540000,gdsId02,500,beijing",
                "orderId01,1573874560000,gdsId01,300,beijing",
                "orderId03,1573874630000,gdsId02,1500,beijing",
                "orderId04,1573874650000,gdsId03,2000,beijing"
        ));

        DataStream<Order> orderStream = stringStream.map((MapFunction<String, Order>) value -> {
            String[] parts = value.split(",");
            long time = System.currentTimeMillis();
//            long time = Long.valueOf(parts[1]);
            return new Order(parts[0], System.currentTimeMillis(), parts[2], Double.valueOf(parts[3]), parts[4]);
        });

        DataStream<Order> salesStream = orderStream
                .keyBy((KeySelector<Order, String>) value -> value.getAreaId() + "_" + value.getGdsId())
                .reduce((ReduceFunction<Order>) (value1, value2) ->
                        new Order(value2.orderId, value2.getOrderTime(), value2.getGdsId(), value1.getAmount() + value2.getAmount(), value2.getAreaId()));

        salesStream
                .keyBy((KeySelector<Order, String>) value -> value.getAreaId())
                .process(new KeyedProcessFunction<String, Order, Void>() {
                    private ValueState<TreeSet> topState;
                    private ValueStateDescriptor<TreeSet> topStateDesc;

//                    private ValueState<Long> fireState;
//                    private ValueStateDescriptor<Long> fireStateDesc;

                    private Long interval = 15 * 1000L;
                    private int N = 3;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        topStateDesc = new ValueStateDescriptor<>("top-state", TypeInformation.of(TreeSet.class));
                        topState = this.getRuntimeContext().getState(topStateDesc);

//                        fireStateDesc = new ValueStateDescriptor<>("fire-state", TypeInformation.of(Long.class));
//                        fireState = this.getRuntimeContext().getState(fireStateDesc);
                    }

                    @Override
                    public void processElement(Order value, Context ctx, Collector<Void> out) throws Exception {
                        TreeSet<Order> tops = topState.value();
                        if (tops == null) {
                            tops = new TreeSet<>((o1, o2) -> {
                                // 构建大堆
                                return Double.compare(o2.getAmount(), o1.getAmount());
                            });
                        }

                        // 方案一：实现按Value排序的Map，类似TreeMap（按Key排序)
                        // TODO

                        // 方案二：遍历大顶堆元素
                        boolean gdsIdNotExistInTops = true;
                        Iterator<Order> it = tops.iterator();
                        while(it.hasNext()) {
                            Order top = it.next();
                            if (top.getGdsId().equals(value.getGdsId())) {
                                gdsIdNotExistInTops = false;
                                it.remove();
                                break;
                            }
                        }

                        if (gdsIdNotExistInTops) {
                            if (tops.size() >= N) {
                                if (value.getAmount() > tops.last().getAmount()) {
                                    // 更新大堆中的最小元素
                                    tops.pollLast();
                                    tops.add(value);
                                    topState.update(tops);
                                }
                            } else {
                                tops.add(value);
                                topState.update(tops);
                            }
                        } else {
                            tops.add(value);
                            topState.update(tops);
                        }

                        // 每来条记录输出
                        System.out.println();
                        System.out.println("=================");
                        System.out.println();
                        topState.value().forEach(x -> System.out.println(x));

                        // 周期性输出
//                        long currTime = ctx.timerService().currentProcessingTime();
//                        if(fireState.value() == null) {
//                            long start = currTime - (currTime % interval);
//                            long nextFireTimestamp = start + interval;
//                            ctx.timerService().registerProcessingTimeTimer(nextFireTimestamp);
//                            fireState.update(nextFireTimestamp);
//                        }
                    }

//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
//                        System.out.println(timestamp +"===");
//                        topState.value().forEach(x -> System.out.println(x));
//                        Long fireTimestamp = fireState.value();
//                        if(fireTimestamp != null && fireTimestamp == timestamp){
//                            fireState.clear();
//                            Long nextFireTimestamp = timestamp + interval;
//                            fireState.update(nextFireTimestamp);
//                            ctx.timerService().registerProcessingTimeTimer(nextFireTimestamp);
//                        }
//                    }

                    @Override
                    public void close() {
                        topState.clear();
//                        fireState.clear();
                    }
                });

        env.execute("GlobalTopNExample");
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
