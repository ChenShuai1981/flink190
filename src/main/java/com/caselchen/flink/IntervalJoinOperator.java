package com.caselchen.flink;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 测试数据
 * <p>
 * User{userId='1001', name='caocao', age='20', sex='null', createTime=1561024902404, updateTime=1561024902404}
 * Order{orderId='1001', userId='1001', price='10', timestamp=1561024905185}
 * (1001,caocao,20,1001,1001,10,1561024905185)
 * Order{orderId='1002', userId='1001', price='20', timestamp=1561024906197}
 * (1001,caocao,20,1001,1002,20,1561024906197)
 * Order{orderId='1003', userId='1002', price='30', timestamp=1561024907198}
 */
public class IntervalJoinOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");

        SingleOutputStreamOperator<Order> order = sEnv
                .addSource(new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), p))
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        return new Gson().fromJson(value, Order.class);
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
                    @Override
                    public long extractAscendingTimestamp(Order element) {
                        return element.timestamp;
                    }
                });

        order.print();


        SingleOutputStreamOperator<User> user = sEnv
                .addSource(new FlinkKafkaConsumer<String>("user", new SimpleStringSchema(), p))
                .map(new MapFunction<String, User>() {
                    @Override
                    public User map(String value) throws Exception {
                        return new Gson().fromJson(value, User.class);
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<User>() {
                    @Override
                    public long extractAscendingTimestamp(User element) {
                        return element.createTime;
                    }
                });

        user.print();


        order
                .keyBy("userId")
                .intervalJoin(user.keyBy("userId"))
                // between 只支持 event time
                .between(Time.seconds(-60), Time.seconds(60))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Order, User, Tuple7<String, String, String, String, String, String, Long>>() {
                    @Override
                    public void processElement(Order order, User user, Context ctx, Collector<Tuple7<String, String, String, String, String, String, Long>> out) throws Exception {
                        out.collect(new Tuple7<>(user.userId, user.name, user.age, order.userId, order.orderId, order.price, order.timestamp));
                    }
                })
                .print();

        sEnv.execute("IntervalJoinOperator");
    }
}