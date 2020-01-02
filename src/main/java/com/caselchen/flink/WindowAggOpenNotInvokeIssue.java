package com.caselchen.flink;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * flink table窗口聚合的open函数未调用的bug分析
 * https://mp.weixin.qq.com/s/GRt_nnahIh7wmQvTMYIMfg
 */
public class WindowAggOpenNotInvokeIssue {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableConfig tableConfig = tEnv.getConfig();
        tableConfig.setIdleStateRetentionTime(Time.minutes(10),Time.minutes(15));

        tEnv.registerFunction("DateUtil",new DateUtil());
        tEnv.registerFunction("WeightedAvg",new WeightedAvg());

        // 示例数据：{"eventtime": "2019-12-17T11:11:29.555Z", "fruit": "orange", "number": 45}
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("jsontest")
                        .property("bootstrap.servers", "localhost:9092")
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
                                .field("rowtime", Types.SQL_TIMESTAMP)
                                .rowtime(new Rowtime()
                                        .timestampsFromField("eventtime")
                                        .watermarksPeriodicBounded(2000)
                                )
                                .field("fruit", Types.STRING)
                                .field("number", Types.INT)
                )
                .inAppendMode()
                .registerTableSource("source");

        //对应场景一，生成GroupAggProcessFunction，带open方法
        Table table = tEnv.sqlQuery("select fruit, DateUtil(rowtime,'yyyyMMddHH'), WeightedAvg(number, number) from source group by fruit, DateUtil(rowtime,'yyyyMMddHH')");

        //对应场景二，生成AggregateFunction，不带open方法
//        Table table = tEnv.sqlQuery("select fruit, WeightedAvg(number,number), TUMBLE_END(rowtime, INTERVAL '5' SECOND) from source group by fruit,TUMBLE(rowtime, INTERVAL '5' SECOND)");

        tEnv.toRetractStream(table, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println(value.f1.toString());
            }
        });

        env.execute("WindowAggOpenNotInvokeIssue");
    }
}
