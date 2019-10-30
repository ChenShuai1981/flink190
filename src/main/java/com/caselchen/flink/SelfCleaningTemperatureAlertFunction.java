package com.caselchen.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SelfCleaningTemperatureAlertFunction extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>> {

    private ValueState<Double> lastTempState = null;
    private ValueState<Long> lastTimerState = null;
    private Double threshold = null;

    public SelfCleaningTemperatureAlertFunction(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor lastTempDescriptor = new ValueStateDescriptor("lastTemp", Double.class);
        lastTempState = getRuntimeContext().getState(lastTempDescriptor);

        ValueStateDescriptor lastTimerDescriptor = new ValueStateDescriptor("lastTimer", Long.class);
        lastTimerState = getRuntimeContext().getState(lastTimerDescriptor);
    }

    @Override
    public void processElement(SensorReading reading, Context ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        long newTimer = ctx.timestamp() + (3600 * 1000);
        long curTimer = lastTimerState.value();
        ctx.timerService().deleteEventTimeTimer(curTimer);
        ctx.timerService().registerEventTimeTimer(newTimer);
        lastTimerState.update(newTimer);
        Double lastTemp = lastTempState.value();
        Double tempDiff = Math.abs(reading.temperature - lastTemp);
        if (tempDiff > threshold) {
            // temperature increased by more than the thresholdTimer
            out.collect(new Tuple3<>(reading.id, reading.temperature, tempDiff));
        }
        this.lastTempState.update(reading.temperature);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Double, Double>> out) {
        lastTempState.clear();
        lastTimerState.clear();
    }
}
