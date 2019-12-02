package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.InjectionPatternFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CEPDynamicExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MapStateDescriptor<String, String> patternStateDescriptor = new MapStateDescriptor<>(
                "PatternBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {}));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> patternConsumer = new FlinkKafkaConsumer("pattern_topic", new SimpleStringSchema(), properties);
        patternConsumer.setStartFromLatest();
        FlinkKafkaConsumer<String> inputConsumer = new FlinkKafkaConsumer("input_topic", new SimpleStringSchema(), properties);
        inputConsumer.setStartFromLatest();

//        BroadcastStream<PatternInfo> patternBroadcastStream = env.addSource(patternConsumer).map(new MapFunction<String, PatternInfo>() {
//            @Override
//            public PatternInfo map(String value) throws Exception {
//                return JSON.parseObject(value, PatternInfo.class);
//            }
//        }).broadcast(patternStateDescriptor);

        DataStream<MetricEvent> metricEventStream = env.addSource(inputConsumer).map(
                (MapFunction<String, MetricEvent>) value -> JSON.parseObject(value, MetricEvent.class));

        PatternStream<MetricEvent> patternStream = CEP.injectPattern(metricEventStream, new InjectionPatternFunction() {

            private Connection connection;
            private String path = "/nodeCache";

            @Override
            public void initialize() throws Exception {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "root");

                CuratorFramework zkClient = getZkClient();
                byte[] initData = "initData".getBytes();
                zkClient.delete().forPath(path);
                //创建节点用于测试
                zkClient.create().forPath(path, initData);
                NodeCache nodeCache = new NodeCache(zkClient, path);
                //添加NodeCacheListener监听器
                nodeCache.getListenable().addListener(new NodeCacheListener() {
                    @Override
                    public void nodeChanged() throws Exception {
                        System.out.println("监听到事件变化，当前数据:"+new String(nodeCache.getCurrentData().getData()));
                    }
                });
                //调用start方法开始监听
                nodeCache.start();
            }

            public CuratorFramework getZkClient() {
                String zkServerAddress = "127.0.0.1:2181";
                ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
                CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                        .connectString(zkServerAddress)
                        .sessionTimeoutMs(5000)
                        .connectionTimeoutMs(5000)
                        .retryPolicy(retryPolicy)
                        .build();
                zkClient.start();
                return zkClient;
            }

            @Override
            public Map<String, Pattern> inject() throws Exception {
                Map<String, Pattern> map = new HashMap<>();
                PreparedStatement ps = connection.prepareStatement("select pattern_string from pattern_tbl where pattern_name = ?");
                String patternName = "testPattern";
                ps.setObject(1, patternName);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    String patternString = rs.getString("pattern");
                    Pattern pattern = ScriptEngine.getPattern(patternString, "get");
                    map.put(patternName, pattern);
                }
                return map;
            }

            @Override
            public long getPeriod() {
                return 10*60*1000; // 10分钟
            }
        });

        SingleOutputStreamOperator<String> filter2 = patternStream.select((PatternSelectFunction<MetricEvent, String>) pattern -> "-----------------------------" + pattern.toString());

        filter2.print();

        env.execute("CEPDynamicExample");
    }

    @Data
    static class PatternInfo {
        private String name;
        private String pattern;
    }
}
