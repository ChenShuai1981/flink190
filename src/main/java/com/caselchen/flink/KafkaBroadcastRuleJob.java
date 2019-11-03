package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaBroadcastRuleJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        FlinkKafkaConsumer<String> ruleConsumer = new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), kafkaConfig);
        DataStream<Rule> ruleStream = env.addSource(ruleConsumer).map(new MapFunction<String, Rule>() {
            @Override
            public Rule map(String value) throws Exception {
                String[] parts = value.split(",");
                return new Rule(parts[0], Boolean.valueOf(parts[1]));
            }
        });
        MapStateDescriptor broadcastStateDesc = new MapStateDescriptor<String,Rule>("broadcast-state",
                BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(Rule.class));
        BroadcastStream<Rule> broadcastRuleStream = ruleStream.broadcast(broadcastStateDesc);

        FlinkKafkaConsumer<String> userActionConsumer = new FlinkKafkaConsumer<String>("topic2", new SimpleStringSchema(), kafkaConfig);
        DataStream<UserAction> userActionStream = env.addSource(userActionConsumer).map(new MapFunction<String, UserAction>() {
            @Override
            public UserAction map(String value) throws Exception {
                String[] parts = value.split(",");
                return new UserAction(parts[0], parts[1], Long.valueOf(parts[2]));
            }
        }).keyBy("userId");
        BroadcastConnectedStream<UserAction, Rule> connectedStream = userActionStream.connect(broadcastRuleStream);
        connectedStream.process(new KeyedBroadcastProcessFunction<String, UserAction, Rule, String>() {
            @Override
            public void processElement(UserAction value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, Rule> state = ctx.getBroadcastState(broadcastStateDesc);
                if(state.contains(value.actionType)) {
                    Tuple4 tupl4 = new Tuple4(value.userId,value.actionType,value.time,"true");
                    out.collect(tupl4.toString());
                }
            }

            @Override
            public void processBroadcastElement(Rule value, Context ctx, Collector<String> out) throws Exception {
                ctx.getBroadcastState(broadcastStateDesc).put(value.actionType,value);
            }
        }).print();
        env.execute("KafkaBroadcastRuleJob");
    }

    @Data
    @AllArgsConstructor
    static class Rule {
        private String actionType;
        private Boolean isActive;
    }

    @Data
    @AllArgsConstructor
    static class UserAction {
        private String userId;
        private String actionType;
        private Long time;
    }
}
