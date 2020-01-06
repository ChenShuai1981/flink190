package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

// https://mp.weixin.qq.com/s/uJAKt8i2qpcRiAKlWp49NQ
public class Kafka2Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("jsontest")
                        .property("bootstrap.servers", "localhost:9093")
                        .property("group.id","test")
                        .startFromLatest()
        )
                .withFormat(
                        new Json()
                                .failOnMissingField(false)
                                .deriveSchema()
                )
                .withSchema(

                        new Schema()
                                .field("rowtime",Types.SQL_TIMESTAMP)
                                .rowtime(new Rowtime()
                                        .timestampsFromField("eventtime")
                                        .watermarksPeriodicBounded(2000)
                                )
                                .field("fruit", Types.STRING)
                                .field("number", Types.INT)
                )
                .inAppendMode()
                .registerTableSource("source");

        tEnv.connect(
                new Kafka()
                        .version("universal")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("test")
                        .property("acks", "all")
                        .property("retries", "0")
                        .property("batch.size", "16384")
                        .property("linger.ms", "10")
                        .property("bootstrap.servers", "localhost:9093")
                        .sinkPartitionerFixed()
        ).inAppendMode()
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("fruit", Types.STRING)
                                .field("total", Types.INT)
                                .field("time", Types.SQL_TIMESTAMP)
                )
                .registerTableSink("sink");

        tEnv.connect(
                new Kafka()
                        .version("universal")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("test")
                        .property("acks", "all")
                        .property("retries", "0")
                        .property("batch.size", "16384")
                        .property("linger.ms", "10")
                        .property("bootstrap.servers", "localhost:9093")
                        .sinkPartitionerFixed()
        ).inAppendMode()
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("fruit", Types.STRING)
                                .field("total", Types.INT)
                                .field("time", Types.SQL_TIMESTAMP)
                )
                .registerTableSink("sink1");

        Table table = tEnv.sqlQuery("select * from source");
        tEnv.registerTable("view",table);


        tEnv.sqlUpdate("insert into sink select fruit,sum(number),TUMBLE_END(rowtime, INTERVAL '5' SECOND) from view group by fruit,TUMBLE(rowtime, INTERVAL '5' SECOND)");
        tEnv.sqlUpdate("insert into sink1 select fruit,sum(number),TUMBLE_END(rowtime, INTERVAL '5' SECOND) from view group by fruit,TUMBLE(rowtime, INTERVAL '5' SECOND)");

        System.out.println(env.getExecutionPlan());
//        env.execute();
    }
}