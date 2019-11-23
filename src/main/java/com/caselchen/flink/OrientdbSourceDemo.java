package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class OrientdbSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo =new RowTypeInfo(new TypeInformation[]{
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        });

        JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.orientechnologies.orient.jdbc.OrientJdbcDriver")
                .setDBUrl("jdbc:orient:remote:localhost/demodb")
                .setUsername("root")
                .setPassword("admin")
                .setQuery("SELECT Id, Name FROM Countries")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        DataStream<Row> ds = env.createInput(jdbcInput, rowTypeInfo);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerDataStream("Countries", ds, "id,name");
        Table query = tableEnv.sqlQuery("select substring(name,1,1), count(*) from Countries group by substring(name,1,1)");

        System.out.println();
        System.out.println(" === Result === ");
        System.out.println();

        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(query, Row.class);
        result.print();

        env.execute("OrientdbSourceDemo");
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class Countries implements Serializable {
        private Long Id;
        private String Name;
    }
}
