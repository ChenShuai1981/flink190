//package com.caselchen.flink;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//
//import java.sql.Timestamp;
//import java.time.Duration;
//import java.time.Instant;
//import java.util.Comparator;
//import java.util.PriorityQueue;
//
//public class CustomWindowExample2 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.setParallelism(1);
//
//        DataStream<Tuple2<String, Timestamp>> callStream = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Tuple2<String, Timestamp>>() {
//            @Override
//            public Tuple2<String, Timestamp> map(String value) throws Exception {
//                return Tuple2.of(value, new Timestamp(System.currentTimeMillis()));
//            }
//        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Timestamp>>(Time.seconds(5)) {
//            @Override
//            public long extractTimestamp(Tuple2<String, Timestamp> element) {
//                return element.f1.getTime();
//            }
//        });
//
//        callStream
//                .keyBy((KeySelector<Tuple2<String, Timestamp>, String>) element -> element.f0)
//                .process(new CallCountFunction()).print();
//
//        env.execute("CustomWindowExample2");
//    }
//
//    static class CallCountFunction extends KeyedProcessFunction<String, Tuple2<String, Timestamp>, Tuple2<String, Long>> {
//
//        // 维持最近30分钟的访问记录
//        private ValueState<PriorityQueue<Tuple2<String, Timestamp>>> state = null;
//        private long ttl = 5 * 1000L;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            ValueStateDescriptor<PriorityQueue<Tuple2<String, Timestamp>>> stateDescriptor = new ValueStateDescriptor("callRecords", TypeInformation.of(new TypeHint<PriorityQueue<Tuple2<String, Timestamp>>>(){}));
//            state = getRuntimeContext().getState(stateDescriptor);
//        }
//
//        @Override
//        public void processElement(Tuple2<String, Timestamp> element, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//            Instant now = Instant.ofEpochMilli(ctx.timestamp());
//            Instant mins30Ago = now.minus(Duration.ofMillis(ttl));
//            Timestamp current = Timestamp.from(now);
//            Timestamp thirtyMinsAgo = Timestamp.from(mins30Ago);
//
//            Timestamp elementTime = element.f1;
//            if ((elementTime.before(current) || elementTime.equals(current)) && (elementTime.after(thirtyMinsAgo) || elementTime.equals(thirtyMinsAgo))) {
//                PriorityQueue<Tuple2<String, Timestamp>> queue = state.value();
//                if (queue == null) {
//                    queue = new PriorityQueue<>(Comparator.comparingLong(o -> o.f1.getTime()));
//                }
//                queue.add(element);
//                state.update(queue);
//                ctx.timerService().registerProcessingTimeTimer(elementTime.getTime() + ttl);
//            }
//
//            long count = state.value().size();
//            out.collect(Tuple2.of(element.f0, count));
//        }
//
//        @Override
//        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//            PriorityQueue<Tuple2<String, Timestamp>> queue = state.value();
//            Tuple2<String, Timestamp> head = queue.peek();
//            if (head.f1.getTime() <= timestamp - ttl) {
//                queue.poll();
//                state.update(queue);
//                long count = state.value().size();
//                out.collect(Tuple2.of(head.f0, count));
//            }
//        }
//    }
//
//}
