package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MultiThreadSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        env.addSource(new SensorSource()).map((MapFunction<SensorReading, String>) value -> {
            String[] strs = new String[]{value.getId(), String.valueOf(value.getTemperature()), String.valueOf(value.getTimestamp())};
            return String.join(",", strs);
        }).addSink(new MultiThreadConsumerSink());

//        env.socketTextStream("localhost", 9999).addSink(new MultiThreadConsumerSink());

        env.execute("MultiThreadSinkJob");
    }
}
