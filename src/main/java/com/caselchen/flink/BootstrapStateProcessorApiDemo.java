package com.caselchen.flink;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import com.caselchen.flink.StateProcessorApiDemo.*;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

import java.util.Arrays;

public class BootstrapStateProcessorApiDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Double>> rates = env.fromCollection(Arrays.asList(new Tuple2<>(0, 1.67), new Tuple2<>(1, 6.43)));
        DataSet<Summary> summary = env.fromCollection(Arrays.asList(new Summary("a1", 2, 2000.0d), new Summary("a2", 3, 300.0d)));

        BootstrapTransformation<Tuple2<Integer, Double>> ratesTransformation = OperatorTransformation
                .bootstrapWith(rates)
                .transform(new RatesBootstrapper());

        BootstrapTransformation<Summary> summaryTransformation = OperatorTransformation
                .bootstrapWith(summary)
                .keyBy(s -> s.accountId)
                .transform(new SummaryBootstrapper());

        StateBackend stateBackend = new FsStateBackend("file:///Users/chenshuai1/flink-checkpoints");

        // 基于历史数据新创建一个savepoint
        String bootstrapSavepointPath = "file:///Users/chenshuai1/flink-checkpoints/bootstrap-savepoints-demo";
        Savepoint
                .create(stateBackend, 128)
                .withOperator("currency_converter", ratesTransformation)
                .withOperator("summarize", summaryTransformation)
                .write(bootstrapSavepointPath);

        env.execute("bootstrap");

    }

    static class SummaryBootstrapper extends KeyedStateBootstrapFunction<String, Summary> {
        ValueState<Double> totalState;
        ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> totalDescriptor = new ValueStateDescriptor<>("total", Types.DOUBLE);
            totalState = getRuntimeContext().getState(totalDescriptor);

            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>("count", Types.INT);
            countState = getRuntimeContext().getState(countDescriptor);
        }


        @Override
        public void processElement(Summary value, Context ctx) throws Exception {
            totalState.update(value.total);
            countState.update(value.count);
        }
    }

    static class RatesBootstrapper extends BroadcastStateBootstrapFunction<Tuple2<Integer, Double>> {
        private MapStateDescriptor<Integer, Double> descriptor =
                new MapStateDescriptor("rates", Types.INT, Types.DOUBLE);
        private BroadcastState ratesState;

        @Override
        public void open(Configuration parameters) throws Exception {
        }

        @Override
        public void processElement(Tuple2<Integer, Double> rate, Context ctx) throws Exception {
            if (ratesState == null) {
                ratesState = ctx.getBroadcastState(descriptor);
            }
            ratesState.put(rate.f0, rate.f1);
        }
    }

}
