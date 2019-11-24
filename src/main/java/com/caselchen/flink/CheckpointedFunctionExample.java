package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class CheckpointedFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setAutoWatermarkInterval(5000);
        env.getCheckpointConfig().setCheckpointInterval(5000);
        env.setParallelism(1);

        DataStream<Tuple2<String, Timestamp>> inputStream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Timestamp>>() {
                    @Override
                    public Tuple2<String, Timestamp> map(String value) throws Exception {
                        return Tuple2.of(value, new Timestamp(System.currentTimeMillis()));
                    }
                });
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);
        tblEnv.registerDataStream("callhistory", inputStream, "name, calltime");
//        Table result = tblEnv.sqlQuery("select name, count(*) from callhistory where calltime > TIMESTAMPADD(SECOND, -5, CURRENT_TIMESTAMP) group by name");
        tblEnv.registerFunction("historycount", new HistoryCount(10));
        Table result = tblEnv.sqlQuery("select name, historycount(calltime) from callhistory group by name");
        tblEnv.toRetractStream(result, Row.class).print();

        env.execute("CheckpointedFunctionExample");
    }

}
