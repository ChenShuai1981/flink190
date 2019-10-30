package com.caselchen.flink;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class FlinkTumblingWindowsJoinDemo {

    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        long delay = 5100L;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 设置数据源
        DataStream<SensorReading> leftSource = env.addSource(new SensorSource()).name("Left Source");
        DataStream<SensorReading> rightSource = env.addSource(new SensorSource()).name("Right Source");

        // 设置水位线
        DataStream<SensorReading> leftStream = leftSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.timestamp;
                    }
                }
        );
        DataStream<SensorReading> rightStream = rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.timestamp;
                    }
                }
        );

        // join 操作
        leftStream.coGroup(rightStream)
                .where(new LeftSelectKey()).equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new LeftJoin())
                .print();

        env.execute("TimeWindowDemo");
    }

    public static class LeftJoin implements CoGroupFunction<SensorReading, SensorReading, Tuple5<String, Double, Double, Long, Long>> {
        @Override
        public void coGroup(Iterable<SensorReading> leftElements, Iterable<SensorReading> rightElements, Collector<Tuple5<String, Double, Double, Long, Long>> out) {
            for (SensorReading leftElem : leftElements) {
                boolean hadElements = false;
                for (SensorReading rightElem : rightElements) {
                    out.collect(new Tuple5<>(leftElem.id, leftElem.temperature, rightElem.temperature, leftElem.timestamp, rightElem.timestamp));
                    hadElements = true;
                }
                if (!hadElements) {
                    out.collect(new Tuple5<>(leftElem.id, leftElem.temperature, 0.0d, leftElem.timestamp, -1L));
                }
            }
        }
    }

    public static class RightJoin implements CoGroupFunction<SensorReading, SensorReading, Tuple5<String, Double, Double, Long, Long>> {
        @Override
        public void coGroup(Iterable<SensorReading> leftElements, Iterable<SensorReading> rightElements, Collector<Tuple5<String, Double, Double, Long, Long>> out) {
            for (SensorReading rightElem : rightElements) {
                boolean hadElements = false;
                for (SensorReading leftElem : leftElements) {
                    out.collect(new Tuple5<>(leftElem.id, leftElem.temperature, rightElem.temperature, leftElem.timestamp, rightElem.timestamp));
                    hadElements = true;
                }
                if (!hadElements) {
                    out.collect(new Tuple5<>(rightElem.id, 0.0d, rightElem.temperature, -1L, rightElem.timestamp));
                }
            }
        }
    }

    public static class InnerJoin implements CoGroupFunction<SensorReading, SensorReading, Tuple5<String, Double, Double, Long, Long>> {
        @Override
        public void coGroup(Iterable<SensorReading> leftElements, Iterable<SensorReading> rightElements, Collector<Tuple5<String, Double, Double, Long, Long>> out) {
            for (SensorReading leftElem : leftElements) {
                for (SensorReading rightElem : rightElements) {
                    out.collect(new Tuple5<>(leftElem.id, leftElem.temperature, rightElem.temperature, leftElem.timestamp, rightElem.timestamp));
                }
            }
        }
    }

    public static class LeftSelectKey implements KeySelector<SensorReading, String> {
        @Override
        public String getKey(SensorReading w) {
            return w.id;
        }
    }

    public static class RightSelectKey implements KeySelector<SensorReading, String> {
        @Override
        public String getKey(SensorReading w) {
            return w.id;
        }
    }
}