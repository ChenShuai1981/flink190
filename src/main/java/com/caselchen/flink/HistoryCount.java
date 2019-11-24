package com.caselchen.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class HistoryCount extends AggregateFunction<Long, List<Timestamp>> implements CheckpointedFunction {

    private ListState<Timestamp> listState = null;
    private List<Timestamp> list = new ArrayList<>();
    private TimingWheel tw = null;

    public HistoryCount(int ttl) {
        this.tw = new TimingWheel(1, ttl, TimeUnit.SECONDS);
    }

    @Override
    public List<Timestamp> createAccumulator() {
        return list;
    }

    @Override
    public Long getValue(List<Timestamp> acc) {
        return tw.size();
//        return 0L;
    }

    public void accumulate(List<Timestamp> acc, Timestamp value) {
        acc.add(value);
//        tw.add(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.update(tw.elements());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor stateDescriptor = new ListStateDescriptor("callState", Timestamp.class);
        listState = context.getKeyedStateStore().getListState(stateDescriptor);
        list = StreamSupport.stream(listState.get().spliterator(), false).collect(Collectors.toList());
        for (Timestamp ts : list) {
            tw.add(ts);
        }
    }
}