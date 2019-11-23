package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.jdbc.SingleJDBCOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class OrientdbSinkDemo {
    public static void main(String[] args) throws Exception {
        // Flink自带的JDBCOutputFormat需要PrepareStatement batch insert支持，可惜OrientDB并不支持，
        // 所以这里自定义一个SingleJDBCOutputFormat，只支持记录来一条插入一条。
        SingleJDBCOutputFormat jdbcOutput = SingleJDBCOutputFormat.buildSingleJDBCOutputFormat()
                .setDrivername("com.orientechnologies.orient.jdbc.OrientJdbcDriver")
                .setDBUrl("jdbc:orient:remote:localhost/demodb")
                .setUsername("root")
                .setPassword("admin")
                .setQuery("INSERT INTO Countries (Id, Name) VALUES (?, ?)")
                .finish();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Countries> inputStream = env.fromElements(
                new Countries(10001L, "WWW"),
                new Countries(10002L, "WEB")
        );

        DataStream<Row> dsRow = inputStream.map((MapFunction<Countries, Row>) value -> {
            Row row = new Row(2);
            row.setField(0, value.getId());
            row.setField(1, value.getName());
            return row;
        });

        dsRow.writeUsingOutputFormat(jdbcOutput);

        env.execute("OrientdbSinkDemo");
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class Countries implements Serializable {
        private Long id;
        private String name;
    }
}
