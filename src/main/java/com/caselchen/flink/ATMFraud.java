package com.caselchen.flink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class ATMFraud {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

//        Properties kafkaConfig = new Properties();
//        kafkaConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kafkaConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        kafkaConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
//
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("atm", new SimpleStringSchema(), kafkaConfig);
//
//        SingleOutputStreamOperator<ATMTrans> atmTransStream = env.addSource(consumer).map(new MapFunction<String, ATMTrans>() {
//            @Override
//            public ATMTrans map(String value) throws Exception {
//                return JSON.parseObject(value, ATMTrans.class);
//            }
//        });

        SingleOutputStreamOperator<ATMTrans> atmTransStream = env.fromElements(
                new ATMTrans("ac_01", "01", 20, "ID_2276369282", "38.6956033", "-121.5922283", Timestamp.valueOf("2018-10-05 16:56:08")),
                new ATMTrans("ac_02", "02", 400, "Flying Pig Bistro", "37.766319", "-122.417422", Timestamp.valueOf("2018-10-05 16:56:08")),
                new ATMTrans("ac_03", "04", 50, "Wells Fargo", "37.5522855", "-121.9797997", Timestamp.valueOf("2018-10-05 16:56:08")),
                new ATMTrans("ac_02", "03", 40, "Flying Pig Bistro", "37.766319", "-122.417422", Timestamp.valueOf("2018-10-05 17:01:59")),
                new ATMTrans("ac_03", "X05", 500, "Barclays", "33.5522855", "-120.9797997", Timestamp.valueOf("2018-10-05 17:01:59"))
        );

        atmTransStream = atmTransStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ATMTrans>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(ATMTrans element) {
                return element.getTs().getTime();
            }
        });

//        atmTransStream.print();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        stEnv.registerDataStream("ATM_TXNS", atmTransStream, "account_id, transaction_id, amount, atm, lat, lon, ts.rowtime");
//
        Table resultTbl = stEnv.sqlQuery("SELECT * FROM ATM_TXNS");
        resultTbl.printSchema();
        stEnv.toAppendStream(resultTbl, Row.class).print();

        env.execute("ATMFraud");
    }

}
