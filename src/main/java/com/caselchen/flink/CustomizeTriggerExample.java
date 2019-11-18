package com.caselchen.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CustomizeTriggerExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)));

        sensorData.keyBy("id")
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .trigger(new CustomizeTrigger(1000, 10000))
                .max("temperature")
                .print();

        env.execute("CustomizeTriggerExample");
    }

    /**
     * 1. early trigger: interval 1 second
     * 2. onTime trigger: when watermark passed window end
     * 3. late trigger: per record
     */
    static class CustomizeTrigger extends Trigger<SensorReading, TimeWindow> {

        private long earlyTriggerIntervalInMillis = 0L;
        private long maxLatenessInMillis = 0L;

        public CustomizeTrigger(long earlyTriggerIntervalInMillis, long maxLatenessInMillis) {
            this.earlyTriggerIntervalInMillis = earlyTriggerIntervalInMillis;
            this.maxLatenessInMillis = maxLatenessInMillis;
        }

        @Override
        public TriggerResult onElement(SensorReading element, long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // firstSeen will be false if not set yet
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Boolean.class));
            if (firstSeen.value() == null) {
                long t = ctx.getCurrentWatermark() + (earlyTriggerIntervalInMillis - (ctx.getCurrentWatermark() % earlyTriggerIntervalInMillis));
                ctx.registerEventTimeTimer(t);
                ctx.registerEventTimeTimer(window.getEnd());
                firstSeen.update(true);
            } else if (time > window.getEnd() && time < window.getEnd() + maxLatenessInMillis) {
                // late trigger per record
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (time == window.getEnd() + maxLatenessInMillis) {
                return TriggerResult.FIRE_AND_PURGE;
            } else if (time > window.getEnd() && time < window.getEnd() + maxLatenessInMillis) {
                return TriggerResult.FIRE;
            } else if (time == window.getEnd()) {
                if (maxLatenessInMillis > 0) {
                    return TriggerResult.FIRE;
                } else {
                    return TriggerResult.FIRE_AND_PURGE;
                }
            } else {
                // register next early firing timer
                long t = ctx.getCurrentWatermark() + (earlyTriggerIntervalInMillis - (ctx.getCurrentWatermark() % earlyTriggerIntervalInMillis));
                if (t < window.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                // fire trigger to evaluate window
                return TriggerResult.FIRE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // clear trigger state
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Boolean.class));
            firstSeen.clear();
        }
    }
}
