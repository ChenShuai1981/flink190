package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// https://www.confluent.io/blog/atm-fraud-detection-apache-kafka-ksql/
public class ATMFraud {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        SingleOutputStreamOperator<ATMTrans> atmTransStream = env.fromElements(
                new ATMTrans("ac_01", "01", 20, "ID_2276369282", 38.6956033, -121.5922283, Timestamp.valueOf("2018-10-05 16:56:08")),
                new ATMTrans("ac_02", "02", 400, "Flying Pig Bistro", 37.766319, -122.417422, Timestamp.valueOf("2018-10-05 16:56:08")),
                new ATMTrans("ac_03", "04", 50, "Wells Fargo", 37.5522855, -121.9797997, Timestamp.valueOf("2018-10-05 16:56:08")),
                new ATMTrans("ac_02", "03", 40, "Flying Pig Bistro", 37.766319, -122.417422, Timestamp.valueOf("2018-10-05 17:01:59")),
                new ATMTrans("ac_03", "X05", 500, "Barclays", 33.5522855, -120.9797997, Timestamp.valueOf("2018-10-05 17:01:59"))
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ATMTrans>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(ATMTrans element) {
                return element.getTs().getTime();
            }
        });

//        SingleOutputStreamOperator<ATMTrans> atmTransStream2 = env.fromElements(
//                new ATMTrans("ac_01", "01", 20, "ID_2276369282", 38.6956033, -121.5922283, Timestamp.valueOf("2018-10-05 16:56:08")),
//                new ATMTrans("ac_02", "02", 400, "Flying Pig Bistro", 37.766319, -122.417422, Timestamp.valueOf("2018-10-05 16:56:08")),
//                new ATMTrans("ac_03", "04", 50, "Wells Fargo", 37.5522855, -121.9797997, Timestamp.valueOf("2018-10-05 16:56:08")),
//                new ATMTrans("ac_02", "03", 40, "Flying Pig Bistro", 37.766319, -122.417422, Timestamp.valueOf("2018-10-05 17:01:59")),
//                new ATMTrans("ac_03", "X05", 500, "Barclays", 33.5522855, -120.9797997, Timestamp.valueOf("2018-10-05 17:01:59"))
//        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ATMTrans>(Time.seconds(5)) {
//            @Override
//            public long extractTimestamp(ATMTrans element) {
//                return element.getTs().getTime();
//            }
//        });

//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
//        stEnv.registerDataStream("ATM_TXNS", atmTransStream, "account_id, transaction_id, amount, atm, lat, lon, ts.rowtime");
//        Table resultTbl = stEnv.sqlQuery("SELECT * FROM ATM_TXNS");
//        resultTbl.printSchema();
//        stEnv.toAppendStream(resultTbl, Row.class).print();

        atmTransStream.keyBy(new AccountKeySelector())
                .intervalJoin(atmTransStream.keyBy(new AccountKeySelector()))
                .between(Time.minutes(0), Time.minutes(10))
                .process(new ProcessJoinFunction<ATMTrans, ATMTrans, ATMTransOut>(){
                    @Override
                    public void processElement(ATMTrans atmTrans1, ATMTrans atmTrans2, Context ctx, Collector<ATMTransOut> out) throws Exception {
                        if (!atmTrans1.getTransaction_id().equals(atmTrans2.getTransaction_id()) && (!atmTrans1.getLat().equals(atmTrans2.getLat()) || !atmTrans1.getLon().equals(atmTrans2.getLon()))) {
                            double distanceKM = LocationUtils.getDistance(atmTrans1.getLat(), atmTrans1.getLon(), atmTrans2.getLat(), atmTrans2.getLon()) / 1000;
                            double secondsDiff = (atmTrans2.getTs().getTime() - atmTrans1.getTs().getTime()) * 1.0d / 1000;
                            double kmh = distanceKM / (secondsDiff / 3600);
                            ATMTransOut atmTransOut = new ATMTransOut(atmTrans1.getTs(), atmTrans2.getTs(), atmTrans1.getAccount_id(), atmTrans2.getAccount_id(), atmTrans1.getTransaction_id(), atmTrans2.getTransaction_id(), atmTrans1.getLat(), atmTrans1.getLon(), atmTrans2.getLat(), atmTrans2.getLon(), distanceKM, secondsDiff, kmh);
                            out.collect(atmTransOut);
                        }
                    }
                }).print();

        env.execute("ATMFraud");
    }

    static class AccountKeySelector implements KeySelector<ATMTrans, String> {

        @Override
        public String getKey(ATMTrans value) throws Exception {
            return value.getAccount_id();
        }
    }

    @Data
    @AllArgsConstructor
    static class ATMTransOut {
        private Timestamp fromTs;
        private Timestamp toTs;
        private String fromAccountId;
        private String toAccountId;
        private String fromTransactionid;
        private String toTransactionid;
        private Double fromLat;
        private Double fromLon;
        private Double toLat;
        private Double toLon;
        private Double distanceKM;
        private Double secondsDiff;
        private Double kmh;
    }

}
