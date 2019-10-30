package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;


public class EvolveStateProcessApiDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        StateBackend stateBackend = new FsStateBackend("file:///Users/chenshuai1/flink-checkpoints");

        // 基于现有的savepoint删除不用的operator再添加新的operator，
        // 最后保存到新的savepoint，以便应用下次重启的时候加载
        String oldSavepointPath = "file:///Users/chenshuai1/flink-checkpoints/savepoint-2de738-04bd09f38d90";
        String newSavepointPath = "file:///Users/chenshuai1/flink-checkpoints/new-savepoints-demo";

        ExistingSavepoint existingSavepoint = Savepoint.load(env, oldSavepointPath, stateBackend);
        DataSet<Tuple2<Integer, Double>> oldRates  = existingSavepoint.readBroadcastState(
                "currency_converter",
                "rates",
                Types.INT, Types.DOUBLE);

        DataSet<Tuple2<Integer, Double>> newRates = oldRates.map(new MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> map(Tuple2<Integer, Double> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1 * 1.2);
            }
        });

        BootstrapTransformation<Tuple2<Integer, Double>> ratesTransformation = OperatorTransformation
                .bootstrapWith(newRates)
                .transform(new BootstrapStateProcessorApiDemo.RatesBootstrapper());

        existingSavepoint
                .removeOperator("currency_converter") // 删除不用的operator
                .withOperator("currency_converter", ratesTransformation) // 更新已有的operator状态
//                .withOperator("new-uid", xxxTransformation) // 添加新的operator状态
                .write(newSavepointPath);

        env.execute("evolve");
    }
}
