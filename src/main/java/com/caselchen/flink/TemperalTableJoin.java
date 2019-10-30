package com.caselchen.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TemperalTableJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceTableName = "RatesHistory";
        CsvTableSource tableSource = CsvTableSourceUtils.genRatesHistorySource();
        tEnv.registerTableSource(sourceTableName, tableSource);

        String SQL = "SELECT * FROM RatesHistory AS r WHERE r.rowtime = ( SELECT MAX(rowtime) FROM RatesHistory AS r2 WHERE r2.currency = r.currency AND r2.rowtime <= '10:58:00'  )";

        Table resultTbl = tEnv.sqlQuery(SQL);
        tEnv.toRetractStream(resultTbl, Row.class).print();

        env.execute();
    }
}
