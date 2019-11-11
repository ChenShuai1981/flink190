package com.caselchen.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class MyHiveTest {
    public static void main(String[] args) throws Exception {
        String catalogName = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/chenshuai1/dev/apache-hive-2.3.6-bin/conf";
        String hiveVersion = "2.3.4";

        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);

        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, hiveVersion);

        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);

        System.out.println(">>>> " + System.currentTimeMillis());
//        tableEnv.sqlQuery("select * from test_tbl").insertInto(catalogName, defaultDatabase, "test_tbl2");
//        tableEnv.sqlUpdate("insert into test_tbl2 select * from test_tbl");

//        tableEnv.sqlUpdate("create table test_tbl4(cnt Long)");
        // 不支持：SQL parse failed. UDT in DDL is not supported yet.
        tableEnv.sqlUpdate("insert into test_tbl3 select count(*) as cnt from test_tbl");
//        tableEnv.sqlQuery("select count(*) from test_tbl2").insertInto(catalogName, defaultDatabase, "test_tbl3");

        tableEnv.execute("MyHiveTest");
        System.out.println("<<<< " + System.currentTimeMillis());

    }
}
