package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.kie.api.runtime.KieSession;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class DroolsBroadcastJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {}));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> ruleConsumer = new FlinkKafkaConsumer("rule_topic", new SimpleStringSchema(), properties);
        ruleConsumer.setStartFromLatest();
        FlinkKafkaConsumer<String> inputConsumer = new FlinkKafkaConsumer("input_topic", new SimpleStringSchema(), properties);
        inputConsumer.setStartFromLatest();

        BroadcastStream<String> ruleBroadcastStream = env.addSource(ruleConsumer).broadcast(ruleStateDescriptor);

        DataStream<Person> inputStream = env.addSource(inputConsumer).map(new MapFunction<String, Person>() {
            @Override
            public Person map(String value) throws Exception {
                return JSON.parseObject(value, Person.class);
            }
        });

        DataStream<String> outputStream = inputStream
                .connect(ruleBroadcastStream)
                .process(
                        new BroadcastProcessFunction<Person, String, String>() {
                            private KieSession ks = null;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                String content = String.join("\n", Files.readAllLines(Paths.get("src/main/resources/rules/sample.drl")));
                                ks = DroolsSessionFactory.createDroolsSessionFromDrl(content);
                            }

                            @Override
                            public void processElement(Person person, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                CompletableFuture.runAsync(() -> {
                                    ks.insert(person);
                                    ks.fireAllRules();
                                    out.collect(person.getResult());
                                });
                            }

                            @Override
                            public void processBroadcastElement(String rule, Context ctx, Collector<String> out) throws Exception {
                                ctx.getBroadcastState(ruleStateDescriptor).put(rule, rule);
                                ks = DroolsSessionFactory.createDroolsSessionFromDrl(rule);
                            }
                        }
                );

        outputStream.print();

        env.execute("DroolsBroadcastJob");
    }

}
