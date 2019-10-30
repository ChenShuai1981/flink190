package com.caselchen.flink;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * coGroup测试数据
 * <p>
 * User{userId='1001', name='caocao', age='20', sex='male', createTime=1561087784197, updateTime=1561087784197}
 * Order{orderId='1001', userId='1001', price='10', timestamp=1561087786816}
 * Order{orderId='1002', userId='1001', price='20', timestamp=1561087787831}
 * Order{orderId='1003', userId='1002', price='30', timestamp=1561087788832}
 * ---------------------------------
 * [Order{orderId='1001', userId='1001', price='10', timestamp=1561087786816}, Order{orderId='1002', userId='1001', price='20', timestamp=1561087787831}]
 * [User{userId='1001', name='caocao', age='20', sex='male', createTime=1561087784197, updateTime=1561087784197}]
 * ---------------------------------
 * [Order{orderId='1003', userId='1002', price='30', timestamp=1561087788832}]
 * []
 */

public class JoinAndCoGroupOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        DataStreamSource<String> order = sEnv.addSource(new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), p));
        DataStreamSource<String> user = sEnv.addSource(new FlinkKafkaConsumer<String>("user", new SimpleStringSchema(), p));

        SingleOutputStreamOperator<Order> operator = order.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                return new Gson().fromJson(value, Order.class);
            }
        });

        operator.print();

        SingleOutputStreamOperator<User> operator1 = user.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                return new Gson().fromJson(value, User.class);
            }
        });

        operator1.print();

        // coGroup既会输出匹配的记过，也会输出未匹配的结果，给出的方式，一个迭代器，需要自己组装。这是和join的区别
        operator.coGroup(operator1)
                .where(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.userId;
                    }
                })
                .equalTo(new KeySelector<User, String>() {
                    @Override
                    public String getKey(User value) throws Exception {
                        return value.userId;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .trigger(ProcessingTimeTrigger.create())
                .apply(new CoGroupFunction<Order, User, Tuple7<String, String, String, String, String, String, Long>>() {
                    @Override
                    public void coGroup(Iterable<Order> first, Iterable<User> second, Collector<Tuple7<String, String, String, String, String, String, Long>> out) throws Exception {
                        System.out.println("---------------------------------");
                        System.out.println(first);
                        System.out.println(second);
                    }
                }).print("coGroup");


        sEnv.execute("JoinOperator");
    }
}