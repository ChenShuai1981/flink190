package com.caselchen.flink;


import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class AlarmFunction extends ProcessFunction<Integer, Integer> {

    private OutputTag<Integer> alarmTag = new OutputTag<Integer>("alarm"){};

    @Override
    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
        if (value < 18) {
            ctx.output(alarmTag, value);
        }
        out.collect(value);
    }

}
