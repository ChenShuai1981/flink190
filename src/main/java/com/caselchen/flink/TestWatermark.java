package com.caselchen.flink;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * https://mp.weixin.qq.com/s/QvpHVFiwgeAKcXqZhcKlvQ
 * 阿粉带你学习Flink中的Watermark
 *
 * 测试数据
 * 001,1575129602000
 * 001,1575129603000
 * 001,1575129601000
 * 001,1575129607000
 * 001,1575129603000
 * 001,1575129605000
 * 001,1575129609000
 * 001,1575129606000
 * 001,1575129612000
 * 001,1575129617000
 * 001,1575129610000
 * 001,1575129616000
 * 001,1575129619000
 * 001,1575129611000
 * 001,1575129618000
 */
public class TestWatermark {
    public static void main(String[] args) throws Exception {
        int port = 9010;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");
        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new WordCountPeriodicWatermarks());
        DataStream<String> window = waterMarkStream.keyBy(0)
                //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .window(TumblingEventTimeWindows.of(Time.seconds(4))) // 4秒一个窗口
//                .allowedLateness(Time.seconds(2)) // 再允许迟到2秒
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrayList = new ArrayList<>();
                        List<String> eventTimeList = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrayList.add(next.f1);
                            eventTimeList.add(String.valueOf(next.f1).substring(8,10));
                        }
                        Collections.sort(arrayList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "\n 键值 : " + key + "\n              " +
                                "触发窗内数据个数 : " + arrayList.size() + "\n              " +
                                "触发窗起始数据：" + sdf.format(arrayList.get(0)) + "\n              " +
                                "触发窗最后（可能是延时）数据：" + sdf.format(arrayList.get(arrayList.size() - 1))
                                + "\n              " +
                                "窗口内的事件数据：" + Joiner.on(",").join(eventTimeList) + "\n" +
                                "实际窗起始和结束时间：" + sdf.format(window.getStart()) + "《----》" + sdf.format(window.getEnd()) + " \n \n ";
                        out.collect(result);
                    }
                });
        window.print();
        env.execute("eventtime-watermark");
    }

    public static class WordCountPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        // 最大允许的乱序时间是 3秒
        private final Long maxOutOfOrderness = 3000L;
        private Long currentMaxTimestamp = 0L;
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            //定义如何提取timestamp
            long timestamp = element.f1;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            long id = Thread.currentThread().getId();
            System.out.println("线程 ID ："+ id +
                    " 键值 :" + element.f0 +
                    ",事件事件:[ "+sdf.format(element.f1)+
                    " ],currentMaxTimestamp:[ "+
                    sdf.format(currentMaxTimestamp)+" ],水印时间:[ "+
                    sdf.format(getCurrentWatermark().getTimestamp())+" ]");
            return timestamp;
        }
    }
}
