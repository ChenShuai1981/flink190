package com.caselchen.flink;

import java.sql.Timestamp;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// https://www.confluent.io/blog/atm-fraud-detection-apache-kafka-ksql/
public class ATMFraudSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple7<String, String, Integer, String, Double, Double, Timestamp>> atmTransStream = env.fromElements(
                Tuple7.of("ac_01", "01", 20, "ID_2276369282", 38.6956033, -121.5922283, Timestamp.valueOf("2018-10-05 16:56:08")),
                Tuple7.of("ac_02", "02", 400, "Flying Pig Bistro", 37.766319, -122.417422, Timestamp.valueOf("2018-10-05 16:56:08")),
                Tuple7.of("ac_03", "04", 50, "Wells Fargo", 37.5522855, -121.9797997, Timestamp.valueOf("2018-10-05 16:56:08")),
                Tuple7.of("ac_02", "03", 40, "Flying Pig Bistro", 37.766319, -122.417422, Timestamp.valueOf("2018-10-05 17:01:59")),
                Tuple7.of("ac_03", "X05", 500, "Barclays", 33.5522855, -120.9797997, Timestamp.valueOf("2018-10-05 17:01:59"))
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple7<String, String, Integer, String, Double, Double, Timestamp>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple7<String, String, Integer, String, Double, Double, Timestamp> element) {
                return element.f6.getTime();
            }
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        stEnv.registerDataStream("ATM_TXNS", atmTransStream, "account_id, transaction_id, amount, atm, lat, lon, ts.rowtime");
        stEnv.registerFunction("geo_distance", new GeoDistanceFunction());

        Table resultTbl = stEnv.sqlQuery("SELECT DATE_FORMAT(T1.ts, 'yyyy-MM-dd HH:mm:ss'), DATE_FORMAT(T2.ts, 'yyyy-MM-dd HH:mm:ss'), " +
                "T1.account_id, T2.account_id, T1.transaction_id, T2.transaction_id, T1.lat, T1.lon, T2.lat, T2.lon, geo_distance(T1.lat, T1.lon, T2.lat, T2.lon)/1000 AS DISTANCE_BETWEEN_TXN_KM, TIMESTAMPDIFF(SECOND, T1.ts, T2.ts) AS SECONDS_DIFFERENCE, ((geo_distance(T1.lat, T1.lon, T2.lat, T2.lon)/1000)*3600)/TIMESTAMPDIFF(SECOND, T1.ts, T2.ts) AS KMH " +
                "FROM ATM_TXNS T1, ATM_TXNS T2 WHERE T1.account_id = T2.account_id " +
                "AND T1.ts BETWEEN T2.ts - INTERVAL '10' MINUTE AND T2.ts " +
                "AND T1.transaction_id <> T2.transaction_id " +
                "AND (T1.lat <> T2.lat OR T1.lon <> T2.lon) " +
                "AND T1.ts <> T2.ts");

        stEnv.toAppendStream(resultTbl, Row.class).print();

        env.execute("ATMFraud");
    }

}
