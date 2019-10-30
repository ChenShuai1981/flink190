//package com.caselchen.flink;
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.functions.TemporalTableFunction;
//import org.apache.flink.types.Row;
//
//import java.sql.Timestamp;
//import java.util.Arrays;
//import java.util.List;
//
//public class TemporalTableJoinTest {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        List<Order> ordersData = Arrays.asList(
//                new Order(2L, "Euro", new Timestamp(2L)),
//                new Order(1L, "US Dollar", new Timestamp(3L)),
//                new Order(50L, "Yen", new Timestamp(4L)),
//                new Order(3L, "Euro", new Timestamp(5L))
//                );
//
//        List<Rate> rateHistoryData = Arrays.asList(
//                new Rate("US Dollar", 102L, new Timestamp(1L)),
//                new Rate("Euro", 114L, new Timestamp(1L)),
//                new Rate("Yen", 1L, new Timestamp(1L)),
//                new Rate("Euro", 116L, new Timestamp(5L)),
//                new Rate("US Dollar", 102L, new Timestamp(1L)),
//                new Rate("Euro", 119L, new Timestamp(7L))
//        );
//
//        DataStream<Order> orderDs = env.fromCollection(ordersData).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {
//            @Override
//            public long extractTimestamp(Order order) {
//                return order.getOrdertime().getTime();
//            }
//        });
//
//        DataStream<Rate> rateDs = env.fromCollection(rateHistoryData).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Rate>(Time.seconds(10)) {
//            @Override
//            public long extractTimestamp(Rate rate) {
//                return rate.getRatetime().getTime();
//            }
//        });
//
//        orderDs.print();
//        rateDs.print();
//
////        tEnv.registerDataStream("Orders", orderDs, "amount, currency, ordertime.rowtime");
////        tEnv.registerDataStream("RatesHistory", rateDs, "currency, rate, ratetime.rowtime");
//
//        Table orders = tEnv.fromDataStream(orderDs, "amount, currency, ordertime.rowtime");
//        Table ratesHistory = tEnv.fromDataStream(rateDs, "currency, rate, ratetime.rowtime");
//
//        tEnv.registerTable("Orders", orders);
//        tEnv.registerTable("RatesHistory", ratesHistory);
//
//        Table tab = tEnv.scan("RatesHistory");
//        TemporalTableFunction temporalTableFunction = tab.createTemporalTableFunction("rateTime", "currency");
//        tEnv.registerFunction("Rates", temporalTableFunction);
//
//        String SQL = "SELECT o.currency, o.amount, r.rate, o.amount * r.rate AS yen_amount FROM Orders AS o, LATERAL TABLE (Rates(o.rowtime)) AS r WHERE r.currency = o.currency";
//
//        tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(SQL));
//        Table resultTbl = tEnv.scan("TemporalJoinResult");
//        DataStream<Row> result = tEnv.toAppendStream(resultTbl, Row.class);
//        result.print();
//
//        env.execute();
//    }
//
//    @Data
//    @AllArgsConstructor
//    public static class Order {
//        private long amount;
//        private String currency;
//        private Timestamp ordertime;
//    }
//
//    @Data
//    @AllArgsConstructor
//    public static class Rate {
//        private String currency;
//        private long rate;
//        private Timestamp ratetime;
//    }
//
//}
