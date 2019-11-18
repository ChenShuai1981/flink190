package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 写入的文件有三种状态：in-process、in-pending、finshed，
 * invoke方法里面正在写入的文件状态是in-process，
 * 当满足滚动策略之后将文件变为in-pending状态，
 * 执行snapshotState方法会对in-process状态文件执行commit操作，
 * 将缓存的数据刷进磁盘，并且记录其当前offset值，
 * 同时会记录in-pending文件的元数据信息，
 * 最终在notifyCheckpointComplete方法中
 * 将记录的in-pending状态文件转换为finished的可读文件。
 */
public class StreamingFileSinkExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(5000L);

        env.addSource(new SensorSource()).map((MapFunction<SensorReading, String>) value -> {
            String[] strs = new String[]{value.getId(), String.valueOf(value.getTemperature()), String.valueOf(value.getTimestamp())};
            return String.join(",", strs);
        }).addSink(
                StreamingFileSink.forRowFormat(new Path("hdfs://localhost:9000/sensordata"), new SimpleStringEncoder<String>())
                        .withBucketAssigner(new BucketAssigner<String, String>() {
                            @Override
                            public String getBucketId(String element, Context context) {
                                long timestamp = Long.valueOf(element.split(",")[2]);
                                Instant instant = Instant.ofEpochMilli(timestamp);
                                LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                                return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
                            }

                            @Override
                            public SimpleVersionedSerializer<String> getSerializer() {
                                return SimpleVersionedStringSerializer.INSTANCE;
                            }
                        })
                        .withRollingPolicy(
                                DefaultRollingPolicy.create()
                                        .withMaxPartSize(1048576L)
                                        .withRolloverInterval(600000L)
                                        .withInactivityInterval(300000L).build()
                        ).build()
        ).setParallelism(1);

        env.execute("StreamingFileSinkExample");
    }
}
