package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RegularTriggerWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> input = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Long>() {
            @Override
            public Long map(String value) throws Exception {
                return Long.valueOf(value);
            }
        });

        input
                .keyBy(r -> r % 10)
                .process(new MyProcessFunction())
                .print();

        env.execute("RegularTriggerWindow");
    }

    // 采用KeyedProcessFunction处理每5分钟统计当天的平均值
    static class MyProcessFunction extends KeyedProcessFunction<Long, Long, Double> {
        MapState<Long, Tuple2<Long, Long>> state;

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state = getRuntimeContext().getMapState(new MapStateDescriptor("dailyAvgData", Types.LONG, Types.TUPLE(Types.LONG, Types.LONG)));
        }

        public void processElement(Long value, Context ctx, Collector<Double> out) throws Exception {
            // 触发计算的间隔
            long interval = Time.seconds(5).toMilliseconds();
            long timer = TimeWindow.getWindowStartWithOffset(ctx.timerService().currentProcessingTime(), 0, interval) + interval;
            ctx.timerService().registerProcessingTimeTimer(timer);
            // 根据消息更新State
            long windowStart = TimeWindow.getWindowStartWithOffset(System.currentTimeMillis(), Time.hours(-8).toMilliseconds(), Time.days(1).toMilliseconds());
            Tuple2<Long, Long> avgData = state.get(windowStart);
            if (avgData == null) {
                avgData = Tuple2.of(0L, 0L);
            }
            avgData.f0 = avgData.f0 + 1;
            avgData.f1 = avgData.f1 + value;
            state.put(windowStart, avgData);
        }

        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Long key = ctx.getCurrentKey();
            // 根据State计算结果
            Tuple2<Long, Long> avgData = state.get(key);
            Double result = avgData.f1 * 1.0d / avgData.f0;
            System.out.println("windowStart = " + key);
            out.collect(result);
        }

    }

}
