package com.caselchen.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StateCreate {

    public static void main(String[] args) throws  Exception{


        final String hostname = "localhost";
        final int port = 9999;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream(hostname, port, "\n");


        env.setParallelism(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(1000 , CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));


        text.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String,Long>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new Tuple2<String,Long>(word, 1L));
                        }
                    }
                })
                .keyBy(0).flatMap(new Sum()).uid("mycount").print();

        env.execute("Socket Window WordCount");



    }

    static class Sum extends RichFlatMapFunction<Tuple2<String,Long>, Tuple2<String,Long>> {
        private transient ValueState<Long> wCount;

        @Override
        public void flatMap(Tuple2<String,Long> wordWithCount, Collector<Tuple2<String,Long>> collector) throws Exception {

            Long currentState = wCount.value();
            Long newState = currentState + wordWithCount.f1;

            wCount.update(newState);

            collector.collect(new Tuple2<String,Long>(wordWithCount.f0, newState));


        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("WordCount", Types.LONG, 0L);
            wCount = getRuntimeContext().getState(descriptor);
        }
    }


}