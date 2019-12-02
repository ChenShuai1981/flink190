package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class CustomWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        DataStream<Tuple2<String, Timestamp>> callStream = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Tuple2<String, Timestamp>>() {
            @Override
            public Tuple2<String, Timestamp> map(String value) throws Exception {
                return Tuple2.of(value, new Timestamp(System.currentTimeMillis()));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Timestamp>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple2<String, Timestamp> element) {
                return element.f1.getTime();
            }
        });

        callStream
                .keyBy((KeySelector<Tuple2<String, Timestamp>, String>) element -> element.f0)
                .process(new CallCountFunction())
                .print();

        env.execute("CustomWindowExample");
    }

    static class CallCountFunction extends KeyedProcessFunction<String, Tuple2<String, Timestamp>, Tuple2<String, Long>> {

        // 维持最近5秒的访问记录
        private ListState<Tuple2<String, Timestamp>> listState = null;
        private long ttl = 10 * 1000L;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Tuple2<String, Timestamp>> listStateDescriptor = new ListStateDescriptor("callRecords", TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));
            listState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Timestamp> element, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            Instant now = Instant.ofEpochMilli(ctx.timestamp());
            Instant mins30Ago = now.minus(Duration.ofMillis(ttl));
            Timestamp current = Timestamp.from(now);
            Timestamp thirtyMinsAgo = Timestamp.from(mins30Ago);

            Timestamp elementTime = element.f1;
            if ((elementTime.before(current) || elementTime.equals(current)) && (elementTime.after(thirtyMinsAgo) || elementTime.equals(thirtyMinsAgo))) {
                listState.add(element);
                long expireTime = elementTime.getTime() + ttl;
                System.out.println("expireTime: " + expireTime);
                ctx.timerService().registerProcessingTimeTimer(expireTime);
            }

            long count = StreamSupport.stream(listState.get().spliterator(), false).count();
            out.collect(Tuple2.of(element.f0, count));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            System.out.println("onTimer: " + timestamp);
            Iterator<Tuple2<String, Timestamp>> it = listState.get().iterator();
            while (it.hasNext()) {
                Tuple2<String, Timestamp> element = it.next();
                if (element.f1.getTime() <= timestamp - ttl) {
                    // 删除过期数据并emit最新状态
                    it.remove();
                    long count = StreamSupport.stream(listState.get().spliterator(), false).count();
                    out.collect(Tuple2.of(element.f0, count));
                }
            }
        }
    }
}
