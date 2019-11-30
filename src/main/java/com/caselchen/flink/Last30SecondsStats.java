package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * CREATE TABLE tmall_item(
 *    itemID VARCHAR,
 *    itemType VARCHAR,
 *    onSellTime TIMESTAMP,
 *    price DOUBLE,
 *    WATERMARK onSellTime FOR onSellTime as withOffset(onSellTime, 0)
 * )
 * WITH (
 *   type = 'sls',
 *    ...
 * ) ;
 *
 * SELECT
 *     itemID,
 *     itemType,
 *     onSellTime,
 *     price,
 *     MAX(price) OVER (
 *         PARTITION BY itemType
 *         ORDER BY onSellTime
 *         RANGE BETWEEN INTERVAL '2' MINUTE preceding AND CURRENT ROW) AS maxPrice
 *   FROM tmall_item
 */
public class Last30SecondsStats {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stblEnv = StreamTableEnvironment.create(env, settings);
        DataStream<CallHistory> callHistoryDataStream = env.socketTextStream("localhost", 9999).map(
                (MapFunction<String, CallHistory>) value -> new CallHistory(value, Timestamp.from(Instant.now())))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CallHistory>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(CallHistory element) {
                        return element.getTs().getTime();
                    }
                });
        stblEnv.registerDataStream("callHistory", callHistoryDataStream, "phoneNo, ts.proctime");
        Table resultTbl = stblEnv.sqlQuery("SELECT phoneNo, count(*) OVER (PARTITION BY phoneNo ORDER BY ts RANGE BETWEEN INTERVAL '1' MINUTE preceding AND CURRENT ROW) AS callCounts FROM callHistory");
        stblEnv.toRetractStream(resultTbl, Row.class).print();
        env.execute("Last30SecondsStats");
    }

}
