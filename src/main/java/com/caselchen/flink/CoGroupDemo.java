package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.Serializable;
import java.util.Properties;

public class CoGroupDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");

        FlinkKafkaConsumer orderConsumer =new FlinkKafkaConsumer<>("topic1",new SimpleStringSchema(), kafkaConfig);
        FlinkKafkaConsumer gdsConsumer =new FlinkKafkaConsumer<>("topic2",new SimpleStringSchema(), kafkaConfig);

        DataStream<Order> orderDs = env.addSource(orderConsumer).map((MapFunction<String, Order>) value -> {
            String[] a = value.split(",");
            return new Order(a[0], a[1], Double.valueOf(a[2]));
        });

        DataStream<Gds> gdsDs = env.addSource(orderConsumer).map((MapFunction<String, Gds>) value -> {
            String[] a = value.split(",");
            return new Gds(a[0], a[1]);
        });

        orderDs.coGroup(gdsDs)
                .where((KeySelector<Order, String>) order -> order.getGdsId()) // orderDs 中选择key
                .equalTo((KeySelector<Gds, String>) gds -> gds.getId()) //gdsDs中选择key
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply((CoGroupFunction<Order, Gds, RsInfo>) (first, second, out) -> {
                    //得到两个流中相同key的集合
                    first.forEach(x -> {
                        if (second.iterator().hasNext()) {
                            second.forEach(y -> {
                                out.collect(new RsInfo(x.id, x.gdsId, x.amount, y.name));
                            });
                        } else {
                            out.collect(new RsInfo(x.id, x.gdsId, x.amount, null));
                        }
                    });
                });

        env.execute("CoGroupDemo");
    }

    @Data
    @AllArgsConstructor
    private static class Order implements Serializable {
        private String id;
        private String gdsId;
        private Double amount;
    }

    @Data
    @AllArgsConstructor
    private static class Gds implements Serializable {
        private String id;
        private String name;
    }

    @Data
    @AllArgsConstructor
    private static class RsInfo implements Serializable {
        private String orderId;
        private String gdsId;
        private Double amount;
        private String gdsName;
    }
}
