package com.caselchen.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class HiveExample {

    private static HiveCatalog hiveCatalog;

    public static void main(String[] args) throws Exception {
        String name = "hive";
        String defaultDatabase = "ods";
        String hiveConfDir = "/Users/chenshuai1/Downloads/hive-dev/conf/";
        String version = "2.3.4";

        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env, bbSettings);


        hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        tableEnv.registerCatalog(name, hiveCatalog);
        tableEnv.useCatalog(name);

//        for (String tbl : bbTableEnv.listTables()) {
//            System.out.println(tbl);
//        }


//        Table result = tableEnv.sqlQuery("select count(*) from test.urget_cnt");
//                ((BatchTableEnvironment) tableEnv).toDataSet(result, Row.class).print();

        /**
         * 结果表，必须是orc的
         * create table test.urget_cnt ( rn bigint) stored as orc;
         *
         */
        System.out.println(">>>> " + System.currentTimeMillis());
        tableEnv.sqlQuery("select count(*) from ods.s02_loan").insertInto("hive", "test", "urget_cnt");


        tableEnv.execute("Flink2HiveBlink");
        System.out.println("<<<< " + System.currentTimeMillis());

    }

}
