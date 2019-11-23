package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class OrientdbSourceDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes =new TypeInformation[]{
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };

        RowTypeInfo rowTypeInfo =new RowTypeInfo(fieldTypes);

        JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.orientechnologies.orient.jdbc.OrientJdbcDriver")
                .setDBUrl("jdbc:orient:remote:localhost/demodb")
                .setUsername("root")
                .setPassword("admin")
                .setQuery("SELECT Id, Name FROM Countries")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        DataSource<Row> ds = env.createInput(jdbcInput);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        tableEnv.registerDataSet("Countries", ds, "Id,Name");
        Table query = tableEnv.sqlQuery("select * from Countries");

        DataSet result = tableEnv.toDataSet(query, Row.class);
        result.print();
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class Countries implements Serializable {
        private Long Id;
        private String Name;
    }
}
