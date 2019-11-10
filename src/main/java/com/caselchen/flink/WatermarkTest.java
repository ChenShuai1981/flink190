package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// https://blog.csdn.net/qq_22222499/article/details/94997611
public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

//        Properties kafkaConfig = new Properties();
//        kafkaConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kafkaConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("watermark-test", new SimpleStringSchema(), kafkaConfig);
//        consumer.assignTimestampsAndWatermarks(new MyWatermarkAssigner());
//        DataStream<String> inputStream = env.addSource(consumer);

        List<WordData> wordDataList = Arrays.asList(
                new WordData("aa", 1562419080000L),
                new WordData("aa", 1562419085000L),
                new WordData("bb", 1562419090000L),
                new WordData("bb", 1562419089000L),
                new WordData("aa", 1562419096000L), // 触发窗口计算了 watermark> window.max()
                new WordData("bb", 1562419097000L),
                new WordData("aa", 1562419080000L), // 这是一个迟到的元素，此时的watermark还小于18:12 还能触发
                new WordData("bb", 1562419080000L), // 这是一个迟到的元素，此时的watermark还小于18:12 还能触发
                new WordData("bb", 1562419098000L), // 现在的watermark> 18:12 了
                new WordData("aa", 1562419080000L)  // 触发不了，之前的窗口已经销毁了
        );

        List<String> wordDataJsonList = wordDataList.stream().map(wd -> JSON.toJSONString(wd)).collect(Collectors.toList());
        DataStream<String> inputStream = env.fromCollection(wordDataJsonList);

        inputStream
                .assignTimestampsAndWatermarks(new MyWatermarkAssigner())
                .map(new MyMapFuction())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))   // 最多允许迟到2s
                .aggregate(new CountAgg(), new WindowResultFunction())
//                .sum(1)
                .print();

        env.execute("WatermarkTest");
    }

    static class WindowResultFunction implements WindowFunction<Long, WordCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<WordCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            String word = ((Tuple1<String>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(WordCount.of(word, window.getStart(), window.getEnd(), count));
        }
    }

    @Data
    static class WordCount {

        private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");

        public String word;     // 商品ID
        public long windowStart;  // 窗口开始时间戳
        public long windowEnd;  // 窗口结束时间戳
        public long count;  // 商品的点击量

        public static WordCount of(String word, long windowStart, long windowEnd, long viewCount) {
            WordCount result = new WordCount();
            result.word = word;
            result.windowStart = windowStart;
            result.windowEnd = windowEnd;
            result.count = viewCount;
            return result;
        }

        private String format(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            LocalDateTime date = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return dtf.format(date);
        }

        @Override
        public String toString() {
            return String.join("\t", new String[]{word, format(windowStart), format(windowEnd), String.valueOf(count)});
        }
    }

    static class CountAgg implements AggregateFunction<Tuple2<String, Integer>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Integer> tuple2, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    static class MyMapFuction implements MapFunction<String, Tuple2> {

        @Override
        public Tuple2 map(String str) throws Exception {
            String value = JSONObject.parseObject(str).getString("word");
//            System.out.println("value is " + value);
            return new Tuple2<>(value, 1);
        }
    }

    static class MyWatermarkAssigner implements AssignerWithPunctuatedWatermarks {

        long maxOutOfOrderness = 6000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Object element, long previousElementTimestamp) {
            long createtime = 0L;
            if (element != null) {
                try {
                    createtime = JSONObject.parseObject((String) element).getLong("createTime");
                    System.out.println("timestamp -> " + createtime);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            currentMaxTimestamp = Math.max(createtime, currentMaxTimestamp);
            return createtime;
        }

        @javax.annotation.Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Object lastElement, long extractedTimestamp) {
            System.out.println("currentWatermark >>>>> " + (currentMaxTimestamp - maxOutOfOrderness));
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

    @Data
    @ToString
    @AllArgsConstructor
    static class WordData implements Serializable {
        private String word;
        private long createTime;
    }
}
