package com.caselchen.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Flink实践|Flink Window的5个使用小技巧
 *
 * https://mp.weixin.qq.com/s/Pmnc1uV-stTZ5LLQhSupcw
 *
 * 窗口去重
 */
public class WindowDedup {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.socketTextStream("localhost", 9999);

        Time gap = Time.minutes(2);

        input
                .keyBy(t -> t.substring(0, 1))
                .window(ProcessingTimeSessionWindows.withGap(gap))
                .trigger(new DedupTrigger(gap.toMilliseconds()))
                .reduce((ReduceFunction<String>) (s, t1) -> s)
                .print();

        env.execute("WindowDedup");
    }

    public static class DedupTrigger extends Trigger<Object, TimeWindow> {
        private long sessionGap;
        private ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<Boolean>("isFirst", Types.BOOLEAN);

        public DedupTrigger(long sessionGap) {
            this.sessionGap = sessionGap;
        }

        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 如果窗口大小和session gap大小相同，则可能为第一条数据，再通过ValueState判断；
            ValueState<Boolean> isFirstState = ctx.getPartitionedState(descriptor);
            if ((window.getEnd() - window.getStart() == sessionGap) && (isFirstState.value() == null || !isFirstState.value())) {
                isFirstState.update(true);
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }

        public boolean canMerge() {
            return true;
        }

        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        }
    }
}
