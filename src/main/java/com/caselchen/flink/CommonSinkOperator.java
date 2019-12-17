package com.caselchen.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink自定义StreamOperator
 * 案例：假设我们现在需要实现一个通用的定时、定量的输出的StreamOperator。
 * 定时输出算子抽象类
 * @param <T>
 */
public abstract class CommonSinkOperator<T extends Serializable> extends AbstractStreamOperator<Object>
        implements ProcessingTimeCallback, OneInputStreamOperator<T, Object> {

    private List<T> list;
    private ListState<T> listState;
    private int batchSize;
    private long interval;
    private ProcessingTimeService processingTimeService;

    public CommonSinkOperator() {
    }

    public CommonSinkOperator(int batchSize, long interval) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = batchSize;
        this.interval = interval;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (interval > 0 && batchSize > 1) {
            processingTimeService = getProcessingTimeService();
            long now=processingTimeService.getCurrentProcessingTime();
            processingTimeService.registerTimer(now + interval, this);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception{
        super.initializeState(context);
        this.list = new ArrayList<T>();
        listState = context.getOperatorStateStore().getSerializableListState("batch-interval-sink");
        if (context.isRestored()) {
            listState.get().forEach(x -> {
                list.add(x);
            });
        }
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        list.add(element.getValue());
        if (list.size() >= batchSize) {
            saveRecords(list);
            list.clear();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (list.size() > 0) {
            listState.clear();
            listState.addAll(list);
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        if (list.size() > 0) {
            saveRecords(list);
            list.clear();
        }
        long now = processingTimeService.getCurrentProcessingTime();
        processingTimeService.registerTimer(now + interval, this);
    }

    public abstract void saveRecords(List<T> datas);
}