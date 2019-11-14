package com.caselchen.flink;

import org.apache.flink.api.common.ExecutionConfig;
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
 * 为了避免自带FixedWindow (TumblingWindow or SlidingWindow) 出现的AlignedTrigger所带来的瞬时物化结果引起的大流量
 * 特别是在元素基数特别大的情况下，这里采用的是UnalignedTrigger方式，也即相同元素id的元素是对齐的，而不同元素id的元素是允许不对齐的
 * 举例 id:1的元素trigger周期是 12:00:00, 13:00:00, 14:00:00...
 *     id:2的元素trigger周期是 12:00:01, 13:00:01, 14:00:01...
 * 这样就将瞬时物化结果引发的大流量平均分担在每个时间点，降低了系统风险
 */
public class UnalignedWindowExample {
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
                .window(UnalignedTumblingEventWindow.of(Time.seconds(5)))
                .reduce(new MaxTempFunction(), new SensorWindowFunction());

        maxTemp.print();

        env.execute("UnalignedWindowExample");
    }

    static class UnalignedTumblingEventWindow extends WindowAssigner<Object, TimeWindow> {
        private long windowSize;

        public UnalignedTumblingEventWindow(Time windowSize) {
            this.windowSize = windowSize.toMilliseconds();
        }

        public static UnalignedTumblingEventWindow of(Time windowSize) {
            return new UnalignedTumblingEventWindow(windowSize);
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long ts, WindowAssignerContext context) {
            SensorReading sr = (SensorReading) element;
            long offset = Math.abs(sr.id.hashCode()) % windowSize;
            long startTime = offset + ts - (ts % windowSize);
            long endTime = startTime + windowSize;
            System.out.println(startTime + " | " + endTime + " --> " + offset + " <-- " + element);
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
