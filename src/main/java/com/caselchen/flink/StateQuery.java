package com.caselchen.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;

/**
 * @author : 恋晨
 * Date  : 2019/9/20 9:14 AM
 * 功能  :
 */
public class StateQuery {

    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final SavepointMetadata metadata;
        final StateBackend stateBackend;

        env.setParallelism(1);

//
//        org.apache.flink.runtime.checkpoint.savepoint.Savepoint savepoint = SavepointLoader.loadSavepoint("hdfs:///flink/checkpoints/f1f60a4d78720ca28e82d64df67144f3/chk-1");
//        int maxParallelism = savepoint
//                .getOperatorStates()
//                .stream()
//                .map(OperatorState::getMaxParallelism)
//                .max(Comparator.naturalOrder())
//                .orElseThrow(() -> new RuntimeException("Savepoint's must contain at least one operator"));
//
//        SavepointMetadata Metadata = new SavepointMetadata(maxParallelism, savepoint.getMasterStates(), savepoint.getOperatorStates());
//
//        StateBackend rocksdb = new RocksDBStateBackend("hdfs:///flink/checkpoints");
//
//
//        OperatorState operatorState = Metadata.getOperatorState("e10adc3949ba59abbe56e057f20f883e");






        ExistingSavepoint existingSavepoint = Savepoint.load(env , "hdfs://127.0.0.1:9000/flink/checkpoints/savepoint-4c226a-660c20b95628" ,
                new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));

        existingSavepoint.readKeyedState("mycount" , new MyKeyedStateReaderFunction()).print();

//
//
//
//
//
//
//
//        KeyedStateInputFormat<StateCreate.WordWithCount, Tuple2<String,Long>> inputFormat = new KeyedStateInputFormat<>(
//                operatorState,
//                rocksdb,
//                Types.POJO(StateCreate.WordWithCount.class),
//                new MyKeyedStateReaderFunction());
//
//        env.createInput(inputFormat,  TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})).print();
//
//
//
//        System.out.print(operatorState.toString()+"\n");
//        System.out.print(operatorState.getStateSize()+"\n");
//        System.out.println(operatorState.getState(0).toString());

        env.execute();

    }
}