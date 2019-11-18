package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * https://mp.weixin.qq.com/s/ca_jkVGc3uaYxFOqUHgShQ
 */
public class ReadParquet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        String hdfsParquetPath = "hdfs://localhost:9000/parquetFile";

        PrimitiveType word = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "word");
        PrimitiveType count = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "count");

        MessageType schema = new MessageType("t1", word, count);
        DataStream<Row> t1 = env.readFile(new ParquetRowInputFormat(new Path(hdfsParquetPath), schema), hdfsParquetPath);
        t1.map((MapFunction<Row, Object>) value -> value.getField(2)).print().setParallelism(1);

        Configuration configuration = new Configuration(true);

        ParquetFileReader parquetFileReader = ParquetFileReader.readFooter(configuration, new org.apache.hadoop.fs.Path(hdfsParquetPath + "/${first_file_name}"))
        MessageType schema2 = parquetFileReader.getFileMetaData().getSchema();
        System.out.println("readed schema:${schema2}");

        DataStream<Row> t1_one = env.readFile(new ParquetRowInputFormat(new Path(hdfsParquetPath), schema), hdfsParquetPath);
        t1_one.map((MapFunction<Row, Object>) value -> value.getField(1)).print().setParallelism(1);

        env.execute("ReadParquet");
    }
}
