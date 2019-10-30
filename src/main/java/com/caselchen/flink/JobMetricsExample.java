package com.caselchen.flink;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 自定义metrics
 * https://blog.csdn.net/aA518189/article/details/88952910
 */
public class JobMetricsExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> sentenceStream = env.socketTextStream("localhost", 9999);
        DataStream<String> upperStream = sentenceStream.map(new RichMapFunction<String, String>() {
            private transient Counter sentenceCounter;

            @Override
            public void open(Configuration parameters) throws Exception {
                sentenceCounter=  getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("flink_test_metric")
                        .counter("sentenceCounter");
            }

            @Override
            public String map(String value) throws Exception {
                //统计语句个数
                sentenceCounter.inc();
                return value.toUpperCase();
            }
        });

        DataStream<Tuple2<String, Integer>> wordStream = upperStream.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            // 单词的字母个数分布
            private transient Histogram histogram;
            // 单词总个数
            private transient Counter wordCounter;
            // 一个句子中的单词个数占比
            private transient Gauge<Long> gauge;
            private transient long ratio = 0l;
            // 一个句子中的平均单词个数
            private transient Meter meter;

            @Override
            public void open(Configuration config) {
                com.codahale.metrics.Histogram dropwizardHistogram =
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                histogram = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("flink_test_metric")
                        .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));

                wordCounter=  getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("flink_test_metric")
                        .counter("myCounter");

                gauge = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("flink_test_metric")
                        .gauge("myGauge", new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return ratio;
                            }
                        });

                com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
                meter = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("flink_test_metric")
                        .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter));
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                // normalize and split the line
                String[] tokens = value.split("\\W+");
                ratio = tokens.length;
                meter.markEvent(tokens.length);
                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        // 统计词语的个数
                        wordCounter.inc();
                        // 统计词语长度分布
                        histogram.update(token.length());
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        });

        DataStream<Tuple2<String, Integer>> wordCountStream = wordStream.keyBy(0).sum(1);
        wordCountStream.print();

        env.execute("JobMetricsExample");
    }
}
