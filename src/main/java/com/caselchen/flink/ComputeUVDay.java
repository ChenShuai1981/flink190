package com.caselchen.flink;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class ComputeUVDay {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableConfig tableConfig = tEnv.getConfig();
        tableConfig.setIdleStateRetentionTime(Time.minutes(10),Time.minutes(15));

        tEnv.registerFunction("DateUtil",new DateUtil());
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

//        Table sourceTbl = tEnv.scan("source");
//        sourceTbl.printSchema();
//        tEnv.toAppendStream(sourceTbl, Row.class).print();

        // 計算天級別的uv
//        Table table = tEnv.sqlQuery("select  DateUtil(rowtime),count(distinct fruit) from source group by DateUtil(rowtime)");

        // 计算小时级别uv
        Table table = tEnv.sqlQuery("select  DateUtil(rowtime,'yyyyMMddHH'),count(distinct fruit) from source group by DateUtil(rowtime,'yyyyMMddHH')");

        tEnv.toRetractStream(table, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println(value.f1.toString());
            }
        });
//
//        System.out.println(env.getExecutionPlan());
        env.execute("ComputeUVDay");
    }
}
