package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * RetractStream输出到Kafka
 */
public class ShopPV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        kafkaConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("shop-visit", new SimpleStringSchema(), kafkaConfig);

        DataStream<Tuple2<String, Long>> inputStream = env.addSource(consumer).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, System.currentTimeMillis());
            }
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        stEnv.getConfig().setIdleStateRetentionTime(Time.days(1), Time.days(2));
        stEnv.registerDataStream("T", inputStream, "shop_id, ts.rowtime");
        Table tumbleWindowTbl = stEnv.sqlQuery("SELECT DATE_FORMAT(ts, 'yyyy-MM-dd'), shop_id, COUNT(*) as pv FROM T GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd'), shop_id");

        stEnv.toRetractStream(tumbleWindowTbl, Row.class).addSink(new RichSinkFunction<Tuple2<Boolean, Row>>() {

            private Producer<String, String> producer = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("acks", "all");
                props.put("retries", 0);
                props.put("batch.size", 16384);
                props.put("linger.ms", 1);
                props.put("buffer.memory", 33554432);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                producer = new KafkaProducer<>(props);
            }

            @Override
            public void close() throws Exception {
                super.close();
                producer.flush();
                producer.close();
            }

            @Override
            public void invoke(Tuple2<Boolean, Row> tuple2, Context context) throws Exception {
                if (tuple2.f0) {
                    producer.send(new ProducerRecord<>("shop-pv", tuple2.f1.toString()));
                }
            }
        });

        env.execute();
    }
}
