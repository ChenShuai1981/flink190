package com.caselchen.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.sql.Types;
import java.util.Properties;

public class KafkaSinkMysql {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //检查点配置
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(1);

        //kafka配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");//earliest

        env.addSource(new FlinkKafkaConsumer<>(
                "zzz",   //这个 kafka topic 需和生产消息的 topic 一致
                new JSONKeyValueDeserializationSchema(true), props))
                .timeWindowAll(Time.seconds(5))
                .apply(new AWF())
                .addSink(new MySQLTwoPhaseCommitSink(
                        "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true" ,//failOverReadOnly=false
                        "com.mysql.cj.jdbc.Driver",
                        "root",
                        "Chenshuai@123",
                        "insert into employee_lwn (id, name, password, age, salary, department) values (?, ?, ?, ?, ?, ?)",
                        (new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.VARCHAR})));

        env.execute("flink kafka to Mysql");
    }

}