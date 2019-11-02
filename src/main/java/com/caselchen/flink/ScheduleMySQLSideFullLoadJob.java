package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ScheduleMySQLSideFullLoadJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);
        inputStream.map((MapFunction<String, AdData>) value -> {
            String[] parts = value.split(",");
            int tId = Integer.valueOf(parts[0]);
            String clientId = parts[1];
            int actionType = Integer.valueOf(parts[2]);
            long time = Long.valueOf(parts[3]);
            return new AdData(null, tId, clientId, actionType, time);
        }).flatMap(new SideFlatMapFunction()).print();
        env.execute("ScheduleMySQLSideFullLoadJob");
    }
}
