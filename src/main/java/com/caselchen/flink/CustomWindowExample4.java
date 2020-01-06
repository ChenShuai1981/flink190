package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class CustomWindowExample4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        DataStream<Tuple2<String, Timestamp>> callStream = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Tuple2<String, Timestamp>>() {
            @Override
            public Tuple2<String, Timestamp> map(String value) throws Exception {
                return Tuple2.of(value, new Timestamp(System.currentTimeMillis()));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Timestamp>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple2<String, Timestamp> element) {
                return element.f1.getTime();
            }
        });

        StreamTableEnvironment stblEnv = StreamTableEnvironment.create(env);
        stblEnv.registerDataStream("tbl", callStream, "phone, ts");
        stblEnv.registerFunction("udtf", new MyTableFunction());
        Table genTable = stblEnv.sqlQuery("select phone1, flag1, ts1 from tbl, LATERAL TABLE(udtf(phone, ts)) as t(phone1, ts1, flag1)");
        DataStream<Tuple3<String, Integer, Timestamp>> genStream = stblEnv.toAppendStream(genTable, Row.class)
                .map(new MapFunction<Row, Tuple3<String, Integer, Timestamp>>() {
                    @Override
                    public Tuple3<String, Integer, Timestamp> map(Row row) throws Exception {
                        return Tuple3.of((String)row.getField(0), (Integer)row.getField(1), (Timestamp)row.getField(2));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Timestamp>>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Timestamp> element) {
                        return element.f2.getTime();
                    }
                });

        stblEnv.registerDataStream("tbl1", genStream, "phone1, flag1, ts1, pt.proctime");

        String query = "SELECT phone1, SUM(flag1) OVER (PARTITION BY phone1 ORDER BY pt RANGE BETWEEN INTERVAL '30' SECOND preceding AND CURRENT ROW) AS last30SecondsCount FROM tbl1";
        Table resultTable = stblEnv.sqlQuery(query);
        stblEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }

    public static class MyTableFunction extends TableFunction<Row> {

        public void eval(String phone, Timestamp t) {
            Timestamp t0 = addSeconds(t, -1);
            Timestamp t1 = addSeconds(t, 1);
            Timestamp t2 = addSeconds(t, 31);

            collect(createRow(phone, t0, 0));
            collect(createRow(phone, t1, 1));
            collect(createRow(phone, t2, 0));
        }

        private Row createRow(String phone, Timestamp ts, Integer flag) {
            Row row = new Row(3);
            row.setField(0, phone);
            row.setField(1, ts);
            row.setField(2, flag);
            return row;
        }

        @Override
        public TypeInformation<Row> getResultType() {
            return Types.ROW(Types.STRING(), Types.SQL_TIMESTAMP(), Types.INT());
        }

        private Timestamp addSeconds(Timestamp oldTime, long seconds) {
            ZonedDateTime zonedDateTime = oldTime.toInstant().atZone(ZoneId.systemDefault());
            Timestamp newTime = Timestamp.from(zonedDateTime.plus(seconds, ChronoUnit.SECONDS).toInstant());
            return newTime;
        }
    }

}
