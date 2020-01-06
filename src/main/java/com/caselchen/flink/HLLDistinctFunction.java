package com.caselchen.flink;


import org.apache.flink.table.functions.AggregateFunction;

public class HLLDistinctFunction extends AggregateFunction<Long, HyperLogLog> {

    @Override
    public HyperLogLog createAccumulator() {
        return new HyperLogLog(0.001);
    }

    public void accumulate(HyperLogLog hll, String id) {
        hll.offer(id);
    }

    @Override
    public Long getValue(HyperLogLog accumulator) {
        return accumulator.cardinality();
    }

}