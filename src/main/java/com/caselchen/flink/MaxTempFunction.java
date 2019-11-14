package com.caselchen.flink;

import org.apache.flink.api.common.functions.ReduceFunction;

public class MaxTempFunction implements ReduceFunction<SensorReading> {

    @Override
    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
        return new SensorReading(value1.id, value2.timestamp, Math.max(value1.getTemperature(), value2.getTemperature()));
    }
}
