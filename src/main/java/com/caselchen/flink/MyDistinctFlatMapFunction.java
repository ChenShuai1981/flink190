package com.caselchen.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

public class MyDistinctFlatMapFunction extends RichFlatMapFunction<Integer, Long> {

    private RoaringBitmap rb;
    private ValueState<RoaringBitmap> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        rb = new RoaringBitmap();
        ValueStateDescriptor<RoaringBitmap> desc = new ValueStateDescriptor<>("mydistinct", RoaringBitmap.class);
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public void flatMap(Integer value, Collector out) throws Exception {
        rb.add(value);
        state.update(rb);
        out.collect(rb.getLongCardinality());
    }
}
