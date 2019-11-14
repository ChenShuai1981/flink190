package com.caselchen.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

/**
 * 每个元素的窗口大小各不相同
 */
public class PerElementWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000);
        env.setParallelism(1);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)));

        DataStream<SensorReadingWindowOutput> maxTemp = sensorData
                .map((MapFunction<SensorReading, SensorReading>) r ->
                        new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
                .keyBy((KeySelector<SensorReading, String>) sr -> sr.id)
                .window(new PerElementWindow())
                .reduce(new MaxTempFunction(), new SensorWindowFunction());

        maxTemp.print();

        env.execute("PerElementWindowExample");
    }

    static class PerElementWindow extends WindowAssigner<Object, TimeWindow> {

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long ts, WindowAssignerContext context) {
            SensorReading sr = (SensorReading) element;
            // 每个元素的windowSize不同
            int idx = Integer.valueOf(sr.id.split("_")[1]);
            long windowSize = 5000 + idx;
            long startTime = ts - (ts % windowSize);
            long endTime = startTime + windowSize;
//            System.out.println(startTime + " | " + endTime + " --> " + windowSize + " <-- " + element);
            return Collections.singletonList(new TimeWindow(startTime, endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }
}
