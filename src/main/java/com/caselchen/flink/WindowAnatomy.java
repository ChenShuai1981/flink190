package com.caselchen.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WindowAnatomy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.socketTextStream("localhost",9999);
//        DataStream<String> source = env.fromElements("hello how are you", "hello how are you", "hello how are you", "hello how are you");
        DataStream<Tuple2<String, Long>> values = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                Arrays.stream(value.split("\\s+")).forEach(k -> out.collect(k));
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return new Tuple2(value, 1L);
            }
        });
        KeyedStream<Tuple2<String, Long>, Tuple> keyValue = values.keyBy(0);

//        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> countWindowWithoutPurge =
//                keyValue.window(GlobalWindows.create()).trigger(CountTrigger.of(2));
//        countWindowWithoutPurge.sum(1).print();

        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> countWindowWithPurge =
                keyValue.window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(2)));
        countWindowWithPurge.sum(1).print();

        env.execute("WindowAnatomy");

    }
}
