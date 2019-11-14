package com.caselchen.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;

/**
 * 在系统自带的SessionWindow基础上增加了最长session时长的限制
 */
public class BoundedSessionWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(10000);
        env.setParallelism(1);

        DataStream<SensorReading> sensorData = env.fromElements(
                new SensorReading("sensor_1", 10000L, 78.0d),
                new SensorReading("sensor_1", 14000L, 78.0d),
                new SensorReading("sensor_1", 18000L, 78.0d),
                new SensorReading("sensor_1", 22000L, 88.0d),
                new SensorReading("sensor_1", 26000L, 88.0d),
                new SensorReading("sensor_1", 30000L, 88.0d)
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
            @Override
            public long extractAscendingTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });

//        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
//                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)));

        DataStream<SensorReadingWindowOutput> maxTemp = sensorData
                .map((MapFunction<SensorReading, SensorReading>) r ->
                        new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
                .keyBy((KeySelector<SensorReading, String>) sr -> sr.id)
                .window(EventTimeBoundedSessionWindow.withGap(Time.seconds(5), Time.seconds(10)))
                .reduce(new MaxTempFunction(), new SensorWindowFunction());

        maxTemp.print();

        env.execute("BoundedSessionWindowExample");
    }

    static class EventTimeBoundedSessionWindow extends MergingWindowAssigner<Object, TimeWindow> {

        private long sessionTimeout;
        private long boundedSessionTimeout;

        public EventTimeBoundedSessionWindow(long sessionTimeout, long boundedSessionTimeout) {
            if (sessionTimeout <= 0) {
                throw new IllegalArgumentException("EventTimeBoundedSessionWindow parameters must satisfy 0 < size");
            }
            if (boundedSessionTimeout <= 0) {
                throw new IllegalArgumentException("EventTimeBoundedSessionWindow parameters must satisfy 0 < boundedSize");
            }

            this.sessionTimeout = sessionTimeout;
            this.boundedSessionTimeout = boundedSessionTimeout;
        }

        public static EventTimeBoundedSessionWindow withGap(Time size, Time boundedSize) {
            return new EventTimeBoundedSessionWindow(size.toMilliseconds(), boundedSize.toMilliseconds());
        }

        @Override
        public void mergeWindows(Collection<TimeWindow> windows, MergeCallback<TimeWindow> callback) {
            List<TimeWindow> sortedWindows = new ArrayList<>();
            for (TimeWindow window : windows) {
                sortedWindows.add(window);
            }
            Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
                @Override
                public int compare(TimeWindow o1, TimeWindow o2) {
                    return Long.compare(o1.getStart(), o2.getStart());
                }
            });

            List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
            Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

            for (TimeWindow candidate: sortedWindows) {
                if (currentMerge == null) {
                    currentMerge = new Tuple2<>();
                    currentMerge.f0 = candidate;
                    currentMerge.f1 = new HashSet<>();
                    currentMerge.f1.add(candidate);
                } else if (currentMerge.f0.intersects(candidate) && inboundSession(currentMerge, candidate, boundedSessionTimeout)) {
                    currentMerge.f0 = currentMerge.f0.cover(candidate);
                    currentMerge.f1.add(candidate);
                } else {
                    merged.add(currentMerge);
                    currentMerge = new Tuple2<>();
                    currentMerge.f0 = candidate;
                    currentMerge.f1 = new HashSet<>();
                    currentMerge.f1.add(candidate);
                }
            }

            if (currentMerge != null) {
                merged.add(currentMerge);
            }

            for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
                if (m.f1.size() > 1) {
                    callback.merge(m.f1, m.f0);
                }
            }
        }

        private boolean inboundSession(Tuple2<TimeWindow,Set<TimeWindow>> currentMerge, TimeWindow candidate, long boundedSessionTimeout) {
            TimeWindow cover = currentMerge.f0.cover(candidate);
            long coverSize = cover.getEnd() - cover.getStart();
            return coverSize < boundedSessionTimeout;
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
            return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
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
