package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class UnnestExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        List<People> userData = Arrays.asList(
                new People("Alice", new String[]{"Engineering Manager", "Software Engineer"}),
                new People("Bob", new String[]{"Professor", "Postdoctoral Researcher", "Research Assitant"}),
                new People("Charlie", new String[]{"Sales Intern"}));
        String query = "SELECT name, unnested_position FROM userTab CROSS JOIN UNNEST(positions_held) AS u (unnested_position)";
        DataStream<People> ds = env.fromCollection(userData);
        Table users = tEnv.fromDataStream(ds, "name, positions_held");
        tEnv.registerTable("userTab", users);
        Table resultTbl = tEnv.sqlQuery(query);
        DataStream<Row> result = tEnv.toAppendStream(resultTbl, Row.class);
        result.print();
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class People implements Serializable {
        private String name;
        private String[] positions_held;
    }
}
