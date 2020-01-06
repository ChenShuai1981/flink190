package com.caselchen.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class DailyWindowContinuousTrigger {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.socketTextStream("localhost", 9999);

        input
                .keyBy(t -> t.substring(0, 1))
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(5)))
//                .trigger(new Trigger<String, TimeWindow>() {
//
//                    public TriggerResult onElement(String element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//                        // 触发计算的间隔
//                        long interval = Time.minutes(5).toMilliseconds();
//                        long timer = TimeWindow.getWindowStartWithOffset(ctx.getCurrentProcessingTime(), 0, interval) + interval;
//                        ctx.registerProcessingTimeTimer(timer);
//
//                        return TriggerResult.CONTINUE;
//                    }
//
//                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.FIRE;
//                    }
//
//                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.CONTINUE;
//                    }
//
//                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//                    }
//                })
                .reduce(new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1;
                    }
                })
                .print();

        env.execute("DailyWindowContinuousTrigger");
    }
}
