package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;

public class StreamCsv {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        CsvTableSource csvTableSource = CsvTableSource.builder()
                .path("/Users/chenshuai1/source.csv")
                .field("player_id", Types.LONG)
                .field("team_id", Types.LONG)
                .field("player_name", Types.STRING)
                .field("height", Types.FLOAT)
                .ignoreFirstLine()
                .build();
        tableEnv.registerTableSource("player", csvTableSource);

        CsvTableSink csvTableSink = new CsvTableSink("/Users/chenshuai1/sink1.csv", ",");
        tableEnv.registerTableSink("stats", new String[]{"player_count", "height_sum"}, new TypeInformation[]{Types.LONG, Types.FLOAT}, csvTableSink);

        tableEnv.sqlUpdate("insert into stats select count(player_id) player_count, sum(height) height_sum from player");

        env.execute("StreamCsv");
    }
}
