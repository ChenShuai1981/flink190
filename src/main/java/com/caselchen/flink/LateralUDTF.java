package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class LateralUDTF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        List<String> userData = Arrays.asList("Sunny#8", "Kevin#36", "Panpan#36");
        String query = "SELECT data, name, age FROM userTab, LATERAL TABLE(splitTVF(data)) AS T(name, age)";
        DataStream<String> ds = env.fromCollection(userData);
        Table users = tEnv.fromDataStream(ds, "data");
        tEnv.registerTable("userTab", users);
        tEnv.registerFunction("splitTVF", new SplitTVF());
        Table resultTbl = tEnv.sqlQuery(query);
        DataStream<Row> result = tEnv.toAppendStream(resultTbl, Row.class);
        result.print();
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SimpleUser implements Serializable {
        private String name;
        private int age;
    }

    public static class SplitTVF extends TableFunction<SimpleUser> {

        public void eval(String user) {
            if (user.contains("#")) {
                String[] splits = user.split("#");
                collect(new SimpleUser(splits[0], Integer.valueOf(splits[1])));
            }
        }
    }
}
