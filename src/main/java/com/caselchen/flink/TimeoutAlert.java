package com.caselchen.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import lombok.AllArgsConstructor;
import lombok.Data;

public class TimeoutAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        OutputTag<TimeoutOrder> timeout = new OutputTag<TimeoutOrder>("timeout"){};

        DataStream<Order> orderStream = env.addSource(new FlinkKafkaConsumer<>("order", new SimpleStringSchema(), kafkaConfig)).map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] parts = value.split(",");
                int expireMinutes = 1; // 默认1分钟过期
                if (parts.length > 1) {
                    expireMinutes = Integer.valueOf(parts[1]);
                }
                return new Order(parts[0], expireMinutes);
            }
        });

        DataStream<Shipment> shipmentStream = env.addSource(new FlinkKafkaConsumer<String>("shipment", new SimpleStringSchema(), kafkaConfig)).map(new MapFunction<String, Shipment>() {
            @Override
            public Shipment map(String value) throws Exception {
                return new Shipment(value);
            }
        });

        // 订单产生后的一段时间内仍未产生运单的要进行alert
        SingleOutputStreamOperator<OnTimeOrder> ontimeOrderStream = orderStream.connect(shipmentStream)
                .keyBy((KeySelector<Order, String>) order -> order.getId(), (KeySelector<Shipment, String>) shipment -> shipment.getOrderId())
                .process(new CoProcessFunction<Order, Shipment, OnTimeOrder>() {
                    // 返回超时未运的Order

                    private MapState<String /* orderId */, Order> orderState = null;
                    private MapState<Long, String /* orderId */> timerState = null;
                    private MapState<String /* orderId */, Long> orderTimerState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderState = getRuntimeContext().getMapState(new MapStateDescriptor<>("orderState", String.class, Order.class));
                        timerState = getRuntimeContext().getMapState(new MapStateDescriptor<>("timerState", Long.class, String.class));
                        orderTimerState = getRuntimeContext().getMapState(new MapStateDescriptor<>("orderTimerState", String.class, Long.class));
                    }

                    @Override
                    public void processElement1(Order order, Context ctx, Collector<OnTimeOrder> out) throws Exception {
                        String orderId = order.getId();
                        if (!orderState.contains(orderId)) {
                            orderState.put(orderId, order);
                            long timerTimestamp = ctx.timerService().currentProcessingTime() + order.getExpireMinutes() * 60 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
//                            System.out.println("registerProcessingTimeTimer -> " + timerTimestamp + "," + orderId);
                            timerState.put(timerTimestamp, orderId);
                            orderTimerState.put(orderId, timerTimestamp);
                        }
                    }

                    @Override
                    public void processElement2(Shipment shipment, Context ctx, Collector<OnTimeOrder> out) throws Exception {
                        String orderId = shipment.getOrderId();
                        if (orderState.contains(orderId)) {
                            OnTimeOrder onTimeOrder = new OnTimeOrder(orderId);
                            out.collect(onTimeOrder);

                            long timerTimestamp = orderTimerState.get(orderId);
                            ctx.timerService().deleteProcessingTimeTimer(timerTimestamp);
//                            System.out.println("deleteProcessingTimeTimer -> " + timerTimestamp + "," + orderId);
                            timerState.remove(timerTimestamp);
                            orderTimerState.remove(orderId);
                            orderState.remove(orderId);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OnTimeOrder> out) throws Exception {
                        String orderId = timerState.get(timestamp);
//                        System.err.println("onTimer -> " + timestamp + "," + orderId);
                        TimeoutOrder timeoutOrder = new TimeoutOrder(orderId);
                        orderState.remove(orderId);
                        timerState.remove(timestamp);
                        orderTimerState.remove(orderId);

                        ctx.output(timeout, timeoutOrder);
                    }

                });

        DataStream<TimeoutOrder> timeoutOrderStream = ((SingleOutputStreamOperator<OnTimeOrder>) ontimeOrderStream).getSideOutput(timeout);

        // sink OnTimeOrders
        ontimeOrderStream.print();

        // sink TimeoutOrders
        timeoutOrderStream.print();

        env.execute("TimeoutAlert");
    }

    @Data
    @AllArgsConstructor
    static class Order {
        private String id;
        private int expireMinutes;
    }

    @Data
    @AllArgsConstructor
    static class Shipment {
        private String orderId;
    }

    @Data
    @AllArgsConstructor
    static class OnTimeOrder {
        private String id;
    }

    @Data
    @AllArgsConstructor
    static class TimeoutOrder {
        private String id;
    }

}
