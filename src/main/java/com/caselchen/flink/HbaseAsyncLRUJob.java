package com.caselchen.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HbaseAsyncLRUJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 跟kafka source topic分区数保持一致
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        DataStream<String> inputStream = env.addSource(kafkaConsumer);

        String zk = "localhost:2181";
        String tableName = "country";
        long maxSize = 0L;
        long ttl = 60*1000L;
        int batchSize = 5;
        long timerDelay = 1000;
        long timerPeriod = 5000;
        HbaseAsyncLRU asyncFunc = new HbaseAsyncLRU(zk, tableName, maxSize, ttl, batchSize, timerDelay, timerPeriod);

        // 1. 要确保这里设置的 timeout时间 > HbaseAsyncLRU中timer定时器的时间 + 一个batchGet请求hbase来回时间
        // 2. 这里设置的capacity参数值 = 攒批大小 * 并发连接数
        DataStream<String> outputStream = AsyncDataStream.unorderedWait(inputStream, asyncFunc,
                6000, TimeUnit.MILLISECONDS, 100);

        outputStream.print();

        env.execute("HbaseAsyncLRUJob");

    }
}
