package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class StateProcessorApiDemo {

    private static MapStateDescriptor<Integer, Double> CURRENCY_RATES =
            new MapStateDescriptor<>("rates", Types.INT, Types.DOUBLE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        DataStream<Transaction> transactions = env.socketTextStream("localhost", 9999)
                .map((MapFunction<String, Transaction>) value -> {
                    String[] items = value.split(",");
                    return new Transaction(items[0], Integer.valueOf(items[1]), Double.valueOf(items[2]));
                });

        BroadcastStream<CurrencyRate> rates = env.socketTextStream("localhost", 9998)
                .map((MapFunction<String, CurrencyRate>) value -> {
                    String[] items = value.split(",");
                    return new CurrencyRate(Integer.valueOf(items[0]), Double.valueOf(items[1]));
                })
                .broadcast(CURRENCY_RATES);

        DataStream<Summary> summaryDataStream = transactions
                .connect(rates)
                .process(new CurrencyConverter())
                .uid("currency_converter")
                .keyBy(transaction -> transaction.accountId)
                .flatMap(new Summarize())
                .uid("summarize");
        summaryDataStream.print();
        env.execute("StateProcessorApiDemo");
    }

    static class CurrencyConverter extends BroadcastProcessFunction<Transaction, CurrencyRate, Transaction> {

        private Map<Integer, List<Transaction>> unmatched = new HashMap();

        @Override
        public void processElement(Transaction value,
                                   ReadOnlyContext ctx,
                                   Collector<Transaction> out) throws Exception {

            Double rate = ctx.getBroadcastState(CURRENCY_RATES).get(value.currencyId);
            if (rate != null) {
                value.amount *= rate;
                out.collect(value);
            } else {
                List<Transaction> transactions = null;
                if (unmatched.get(value.currencyId) == null) {
                    transactions = new ArrayList<>();
                } else {
                    transactions = unmatched.get(value.currencyId);
                }
                transactions.add(value);
                unmatched.put(value.currencyId, transactions);
            }
        }

        public void processBroadcastElement(
                CurrencyRate value,
                Context ctx,
                Collector<Transaction> out) throws Exception {

            ctx.getBroadcastState(CURRENCY_RATES).put(value.currencyId, value.rate);
            if (unmatched.containsKey(value.currencyId)) {
                List<Transaction> transactions = unmatched.get(value.currencyId);
                for (Transaction transaction : transactions) {
                    transaction.amount *= value.rate;
                    out.collect(transaction);
                }
                unmatched.remove(value.currencyId);
            }
        }
    }

    static class Summarize extends RichFlatMapFunction<Transaction, Summary> {
        transient ValueState<Double> totalState;
        transient ValueState<Integer> countState;

        public void open(Configuration configuration) throws Exception {
            totalState = getRuntimeContext().getState(
                    new ValueStateDescriptor("total", Types.DOUBLE));
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor("count", Types.INT));
        }

        public void flatMap(Transaction value, Collector<Summary> out) throws Exception {
            Summary summary = new Summary();
            summary.accountId = value.accountId;
            summary.total = value.amount;
            summary.count = 1;

            Double currentTotal = totalState.value();
            if (currentTotal != null) {
                summary.total += currentTotal;
            }

            Integer currentCount = countState.value();
            if (currentCount != null) {
                summary.count += currentCount;
            }
            totalState.update(summary.total);
            countState.update(summary.count);

            out.collect(summary);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class CurrencyRate {
        public int currencyId;
        public double rate;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Transaction {
        public String accountId;
        public int currencyId;
        public double amount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Summary {
        public String accountId;
        public int count;
        public double total;
    }
}
