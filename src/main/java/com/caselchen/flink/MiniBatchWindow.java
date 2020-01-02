package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Mini-Batch输出
 */
public class MiniBatchWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                });

        input
                .keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                .trigger(new BatchSendTrigger(100, 5000))
                .process(new BatchSendFunction());


        env.execute("MiniBatchWindow");
    }

    public static class BatchSendFunction extends ProcessWindowFunction {

        @Override
        public void open(Configuration parameters) throws Exception {

        }

        public void process(Object o, Context context, Iterable elements, Collector out) throws Exception {
            List<Object> batch = new ArrayList<>();
            for(Object e: elements) {
                batch.add(e);
            }

            // batch发送
//            client.send(batch);
        }
    }

    public static class BatchSendTrigger<T> extends Trigger<T, GlobalWindow> {
        // 最大缓存消息数量
        long maxCount;
        // 最大缓存时长
        long maxDelay;

        // 当前消息数量
        int elementCount;
        // processing timer的时间
        long timerTime;

        public BatchSendTrigger(long maxCount, long maxDelay) {
            this.maxCount = maxCount;
            this.maxDelay = maxDelay;
        }

        public TriggerResult onElement(T element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            if (elementCount == 0) {
                timerTime = ctx.getCurrentProcessingTime() + maxDelay;
                ctx.registerProcessingTimeTimer(timerTime);
            }

            // maxCount条件满足
            if (++elementCount >= maxCount) {
                elementCount = 0;
                ctx.deleteProcessingTimeTimer(timerTime);
                return TriggerResult.FIRE_AND_PURGE;
            }

            return TriggerResult.CONTINUE;
        }

        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // maxDelay条件满足
            elementCount = 0;
            return TriggerResult.FIRE_AND_PURGE;
        }

        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        }
    }
}
