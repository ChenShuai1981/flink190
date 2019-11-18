package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;

public class WriteParquet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        String hdfsParquetPath = "hdfs://localhost:9000/parquetFile";
        DataStream<Word> wc = env
                .socketTextStream("localhost", 9999)
                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    String[] strs = value.split("\\s");
                    for (String str : strs) {
                        out.collect(str);
                    }
                })
                .filter((FilterFunction<String>) value -> !StringUtils.isBlank(value))
                .map((MapFunction<String, Word>) value -> new Word(value, 1))
                .keyBy((KeySelector<Word, String>) value -> value.getWord())
                .timeWindow(Time.seconds(3))
                .sum("count");

        StreamingFileSink<Word> parquetSink = StreamingFileSink
                .forBulkFormat(new Path(hdfsParquetPath), ParquetAvroWriters.forReflectRecord(Word.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>()).build();


        wc.addSink(parquetSink).setParallelism(1);

        env.execute("WriteParquet");
    }

    @Data
    @AllArgsConstructor
    static class Word implements Serializable {
        private String word;
        private long count;
    }
}
