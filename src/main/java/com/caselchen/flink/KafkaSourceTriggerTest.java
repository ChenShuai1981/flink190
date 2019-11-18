package com.caselchen.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceTriggerTest {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        AllWindowedStream<Integer, TimeWindow> stream = env
                .addSource(kafkaConsumer)
                .map(new String2Integer())
                .timeWindowAll(Time.seconds(20))
                .trigger(CustomProcessingTimeTrigger.create());

        stream.sum(0).print();

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static class String2Integer extends RichMapFunction<String, Integer> {
        private static final long serialVersionUID = 1180234853172462378L;

        @Override
        public Integer map(String event) throws Exception {

            return Integer.valueOf(event);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
        }
    }

    private static class CustomProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private CustomProcessingTimeTrigger() {
        }

        private static int flag = 0;

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
            // CONTINUE是代表不做输出，也即是，此时我们想要实现比如10条输出一次，
            // 而不是窗口结束再输出就可以在这里实现。
            if (flag > 9) {
                flag = 0;
                return TriggerResult.FIRE;
            } else {
                flag++;
            }
            System.out.println("onElement : " + element);
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            // only register a timer if the time is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the time is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                ctx.registerProcessingTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "ProcessingTimeTrigger()";
        }

        /**
         * Creates a new trigger that fires once system time passes the end of the window.
         */
        public static CustomProcessingTimeTrigger create() {
            return new CustomProcessingTimeTrigger();
        }
    }
}
