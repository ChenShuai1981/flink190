package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
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
import java.util.stream.StreamSupport;

/**
 * 参考 https://blog.csdn.net/qq_22222499/article/details/94997611
 *
 * 输出结果示范
 * word,watermark,windowStart,windowEnd,processingTime,eventTime,count
 * ===================================================================
 * aa	00:52:47	21:18:00	21:18:10	15:41:54	21:18:00	1
 * aa	21:17:54	21:18:00	21:18:10	15:41:54	21:18:05	2
 * bb	21:17:59	21:18:10	21:18:20	15:41:54	21:18:10	1
 * bb	21:18:04	21:18:00	21:18:10	15:41:54	21:18:09	1
 * aa	21:18:04	21:18:10	21:18:20	15:41:54	21:18:16	1
 * bb	21:18:10	21:18:10	21:18:20	15:41:54	21:18:17	2
 * aa	21:18:11	21:18:00	21:18:10	15:41:54	21:18:00	3
 * bb	21:18:11	21:18:00	21:18:10	15:41:54	21:18:00	2
 * bb	21:18:11	21:18:10	21:18:20	15:41:54	21:18:18	3
 */
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
        DataStream<String> stringStream = env.fromCollection(wordDataJsonList);

        System.out.println(String.join(",", new String[]{"word", "watermark", "windowStart", "windowEnd", "processingTime", "eventTime", "count"}));
        System.out.println("============================================================================================================================");

        DataStream<WordInput> wordInputStream = stringStream
                .assignTimestampsAndWatermarks(new MyWatermarkAssigner())
                .map(new MyMapFuction());

        wordInputStream.keyBy(wordInput -> wordInput.getWord())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))   // 最多允许迟到2s
                .trigger(CountTrigger.of(1))
                .process(new CountFunction())
                .print();

        env.execute("WatermarkTest");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    static class WordInput implements Serializable {
        private String word;
        private long createTime;
        private int count;

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof WordInput)) {
                return false;
            }
            WordInput input = (WordInput) obj;

            return this.word.equals(input.getWord()) && this.createTime == input.getCreateTime();
        }
    }

    static class CountFunction extends ProcessWindowFunction<WordInput, WordCount, String, TimeWindow> {

        private List<WordInput> lastWordInputs = null;

        @Override
        public void process(String key, Context context, Iterable<WordInput> elements, Collector<WordCount> out) throws Exception {
            String word = key;

            long eventTime = 0L;
            List<WordInput> diff = null;
            if (lastWordInputs == null) {
                diff = ImmutableList.copyOf(elements);
            } else {
                List<WordInput> wordInputs = ImmutableList.copyOf(elements);
                diff = ListUtils.subtract(lastWordInputs, wordInputs);
                lastWordInputs = wordInputs;
            }
            for (WordInput wordInput : diff) {
                eventTime = wordInput.getCreateTime();
            }

            long count = StreamSupport.stream(elements.spliterator(), false).count();
            long watermark = context.currentWatermark();
            long processingTime = context.currentProcessingTime();
            TimeWindow tw = context.window();
            long windowStart = tw.getStart();
            long windowEnd = tw.getEnd();
            WordCount wordOutput = WordCount.of(word, count, watermark, windowStart, windowEnd, processingTime, eventTime);
            out.collect(wordOutput);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    static class WordCount {

        private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");

        private String word;     // 商品ID
        private long watermark;  // 水印
        private long windowStart;  // 窗口开始时间戳
        private long windowEnd;  // 窗口结束时间戳
        private long count;  // 商品的点击量
        private long processingTime; // 处理时间
        private long eventTime; // 事件时间

        public static WordCount of(String word, long count, long watermark, long windowStart, long windowEnd, long processingTime, long eventTime) {
            WordCount wc = new WordCount();
            wc.word = word;
            wc.windowStart = windowStart;
            wc.windowEnd = windowEnd;
            wc.watermark = watermark;
            wc.processingTime = processingTime;
            wc.count = count;
            wc.eventTime = eventTime;
            return wc;
        }

        private String format(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            LocalDateTime date = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            return dtf.format(date);
        }

        @Override
        public String toString() {
            return String.join("\t", new String[]{word, format(watermark), format(windowStart), format(windowEnd), format(processingTime), format(eventTime), String.valueOf(count)});
        }
    }

    static class MyMapFuction implements MapFunction<String, WordInput> {

        @Override
        public WordInput map(String jsonString) throws Exception {
            WordInput wordInput = JSON.parseObject(jsonString, WordInput.class);
            wordInput.setCount(1);
            return wordInput;
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
//                    System.out.println("timestamp -> " + createtime);
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
//            System.out.println("currentWatermark >>>>> " + (currentMaxTimestamp - maxOutOfOrderness));
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
