package com.caselchen.flink;

import com.example.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;

public class DistinctCountJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> inputStream = env.fromCollection(Arrays.asList(1,2,3,4,5,4,5));

        DataStream<Long> distinctCountStream = inputStream.keyBy(new KeySelector<Integer, Object>() {
            public String getKey(Integer value) throws Exception {
                return "aaa";
            }
        }).map(new RichMapFunction<Integer, Long>() {
            private RoaringBitmap rb;
            private ValueState<RoaringBitmap> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                rb = new RoaringBitmap();
                ValueStateDescriptor<RoaringBitmap> desc = new ValueStateDescriptor<>("mydistinct", RoaringBitmap.class);
                state = getRuntimeContext().getState(desc);
            }

            @Override
            public Long map(Integer value) throws Exception {
                rb.add(value);
                state.update(rb);
                return rb.getLongCardinality();
            }
        });

        distinctCountStream.print();

        env.execute("DistinctCountJob");
    }

}
