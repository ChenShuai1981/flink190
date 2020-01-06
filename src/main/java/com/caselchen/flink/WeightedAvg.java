package com.caselchen.flink;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Iterator;

/**
 * Weighted Average user-defined aggregate function.
 */
public class WeightedAvg extends AggregateFunction<Integer, WeightedAvgAccum> {
    @Override
    public void open( FunctionContext context) throws Exception, Exception {
        this.flag =100;
    }

    private int flag =1;

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    @Override
    public Integer getValue(WeightedAvgAccum acc) {
        System.out.println("value of flag  is : "+flag);
        if (acc.count == 0) {
            return null;
        } else {
            int i = acc.sum / acc.count;
            return i;
        }
    }

    public void accumulate(WeightedAvgAccum acc, int iValue, int iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccum acc, int iValue, int iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }

    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            WeightedAvgAccum a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.count = 0;
        acc.sum = 0;
    }
}