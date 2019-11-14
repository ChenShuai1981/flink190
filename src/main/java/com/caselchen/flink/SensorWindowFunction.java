package com.caselchen.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SensorWindowFunction extends ProcessWindowFunction<SensorReading, SensorReadingWindowOutput, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<SensorReading> elements, Collector<SensorReadingWindowOutput> out) throws Exception {
        SensorReading acc = elements.iterator().next();
        TimeWindow tw = context.window();
        long windowStart = tw.getStart();
        long windowEnd = tw.getEnd();
        SensorReadingWindowOutput output = new SensorReadingWindowOutput(acc.getId(), windowStart, windowEnd, acc.getTemperature());
        out.collect(output);
    }
}
