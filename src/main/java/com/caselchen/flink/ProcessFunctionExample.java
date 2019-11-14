package com.caselchen.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/dev/stream/operators/process_function.html
public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // the source data stream
        DataStream<Tuple2<String, String>> stream = env.fromElements(
                new Tuple2<>("aaa", "test1"), new Tuple2<>("aaa", "test2"), new Tuple2<>("aaa", "test1"),
                new Tuple2<>("bbb", "test1"), new Tuple2<>("aaa", "test2"), new Tuple2<>("aaa", "test2"),
                new Tuple2<>("ccc", "test1"), new Tuple2<>("ccc", "test2"), new Tuple2<>("ccc", "test3"));

        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(0)
                .process(new CountWithTimeoutFunction());

        result.print();

        env.execute("ProcessFunctionExample");
    }

    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {

        public String key;
        public long count;
        public long lastModified;
    }

    /**
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    public static class CountWithTimeoutFunction
            extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

        /**
         * The state that is maintained by this process function
         */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.timestamp();

            // write the state back
            state.update(current);

            // schedule the next timer 60 seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 60000) {
                // emit the state on timeout
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }
}
