package com.caselchen.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 相较于ReduceFunction/AggregateFunction/FoldFunction的可以提前对数据进行聚合处理，
 * ProcessWindowFunction是把数据缓存起来，在Trigger触发计算之后再处理
 *
 * 不过需要注意的是，如果同时指定了Evictor的话，那么即使使用 ReduceFunction/AggregateFunction/FoldFunction，
 * Window也会缓存所有数据，以提供给Evictor进行过滤，因此要慎重使用。
 */
public class WindowAggregationDiff {

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
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
//                .process(new AvgProcessWindowFunction())
                .aggregate(new AvgAggregateFunction())
                .print();

        env.execute("WindowAggregationDiff");
    }

    static class AvgProcessWindowFunction extends ProcessWindowFunction<Long, Double, Long, TimeWindow> {
        public void process(Long integer, Context context, Iterable<Long> elements, Collector<Double> out) throws Exception {
            TimeWindow tw = context.window();
            long start = tw.getStart();
            System.out.println("windowStart: " + start);

            int cnt = 0;
            double sum = 0;
            // elements 缓存了所有数据
            for (long e : elements) {
                cnt++;
                sum += e;
            }
            out.collect(sum / cnt);
        }
    }

    static class AvgAggregateFunction implements AggregateFunction<Long, Tuple2<Long, Long>, Double> {
        public Tuple2<Long, Long> createAccumulator() {
            // Accumulator是内容为<SUM, COUNT>的Tuple
            return new Tuple2<>(0L, 0L);
        }

        public Tuple2<Long, Long> add(Long in, Tuple2<Long, Long> acc) {
            return new Tuple2<>(acc.f0 + in, acc.f1 + 1L);
        }

        public Double getResult(Tuple2<Long, Long> acc) {
            return ((double) acc.f0) / acc.f1;
        }

        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc0, Tuple2<Long, Long> acc1) {
            return new Tuple2<>(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1);
        }
    }

}
