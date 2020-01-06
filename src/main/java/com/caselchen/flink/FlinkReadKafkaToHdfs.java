package com.caselchen.flink;

import java.time.ZoneId;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkReadKafkaToHdfs {

    private final static StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static Properties properties = new Properties();

    /**
     *  kafka 中发送数据JSON格式：
     *  {"passing_time":"1546676393000","plate_no":"1"}
     *  
     */
    public static void main(String[] args) throws Exception {
        init();
        readKafkaToHdfsByReflect(environment, properties);
    }

    private static void init() {
        environment.enableCheckpointing(5000);
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //flink consumer flink的消费者的group.id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //第一种方式：路径写自己代码上的路径
//        properties.setProperty("fs.hdfs.hadoopconf", "...\\src\\main\\resources");
        //第二种方式：填写一个scheme参数即可
        properties.setProperty("fs.default-scheme", "hdfs://localhost:9000");

        properties.setProperty("kafka.topic", "prti");
        properties.setProperty("hfds.path", "hdfs://localhost:9000/prti");
        properties.setProperty("hdfs.path.date.format", "yyyy-MM-dd--HH-mm");
        properties.setProperty("hdfs.path.date.zone", "Asia/Shanghai");
        properties.setProperty("window.time.minute", "1");
    }

    public static void readKafkaToHdfsByReflect(StreamExecutionEnvironment environment, Properties properties) throws Exception {
        String topic = properties.getProperty("kafka.topic");
        String path = properties.getProperty("hfds.path");
        String pathFormat = properties.getProperty("hdfs.path.date.format");
        String zone = properties.getProperty("hdfs.path.date.zone");
        Long windowTime = Long.valueOf(properties.getProperty("window.time.minute"));

        FlinkKafkaConsumer<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        KeyedStream<Prti, String> keyedStream = environment.addSource(flinkKafkaConsumer010).setParallelism(1)
            .map(FlinkReadKafkaToHdfs::transformData)
            .assignTimestampsAndWatermarks(new CustomWatermarks<Prti>())
            .keyBy(Prti::getPlateNo);

        DataStream<Prti> output = keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(windowTime)))
                .apply(new WindowFunction<Prti, Prti, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<Prti> iterable, Collector<Prti> collector) throws Exception {
                        System.out.println("keyBy: " + key + ", window: " + timeWindow.toString());
                        iterable.forEach(collector::collect);
                    }
                });

        //写入HDFS，parquet格式
        DateTimeBucketAssigner<Prti> bucketAssigner = new DateTimeBucketAssigner<>(pathFormat, ZoneId.of(zone));
        StreamingFileSink<Prti> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(path), ParquetAvroWriters.forSpecificRecord(Prti.class))
                .withBucketCheckInterval(5000)
                .withBucketAssigner(bucketAssigner)
                .build();
        output.addSink(streamingFileSink).name("Hdfs Sink");
        environment.execute("PrtiData");
    }

    private static Prti transformData(String data) {
        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data);
            Prti prti = new Prti();
            prti.setPlateNo(value.getString("plate_no"));
            prti.setPassingTime(value.getString("passing_time"));
            return prti;
        } else {
            return new Prti();
        }
    }

//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    static class Prti implements Serializable {
//        private String passingTime;
//        private String plateNo;
//    }

    private static class CustomWatermarks<T> implements AssignerWithPunctuatedWatermarks<Prti> {
        private Long currentTime = 0L;

        @Override
        public Watermark checkAndGetNextWatermark(Prti prti, long l) {
            return new Watermark(currentTime);
        }

        @Override
        public long extractTimestamp(Prti prti, long l) {
            Long passingTime = Long.valueOf(prti.getPassingTime());
            currentTime = Math.max(passingTime, currentTime);
            return passingTime;
        }
    }
}