package com.caselchen.flink;

import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class ReadStateProcessorApiDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        StateBackend stateBackend = new FsStateBackend("file:///Users/chenshuai1/flink-checkpoints");

//        String existingSavepointPath = "file:///Users/chenshuai1/flink-checkpoints/savepoint-2de738-04bd09f38d90";
//        String existingSavepointPath = "file:///Users/chenshuai1/flink-checkpoints/bootstrap-savepoints-demo";
        String existingSavepointPath = "file:///Users/chenshuai1/flink-checkpoints/new-savepoints-demo";
        ExistingSavepoint savepoint = Savepoint.load(bEnv, existingSavepointPath, stateBackend);

        DataSet<Tuple2<Integer, Double>> ratesState  = savepoint.readBroadcastState(
                "currency_converter",
                "rates",
                Types.INT, Types.DOUBLE);
        ratesState.print();

        DataSet<KeyedSummaryState> keyedSummaryState = savepoint.readKeyedState(
                "summarize",
                new SummaryReaderFunction());
        keyedSummaryState.print();
    }

    @Data
    @ToString
    static class KeyedSummaryState {
        String key;
        Double total;
        Integer count;
    }

    static class SummaryReaderFunction extends KeyedStateReaderFunction<String, KeyedSummaryState> {
        private ValueState<Double> totalState;
        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) {
            totalState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("total", Double.class));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
        }

        @Override
        public void readKey(
                String key,
                Context ctx,
                Collector<KeyedSummaryState> out) throws Exception {

            KeyedSummaryState data = new KeyedSummaryState();
            data.key    = key;
            data.total  = totalState.value();
            data.count  = countState.value();
            out.collect(data);
        }
    }
}
