package com.caselchen.flink;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickhouseJDBCExample {
    public static void main(String[] args) {
        String sqlDB = "show databases";//查询数据库
        String sqlTab = "show tables";//查看表
        String sqlCount = "select count(*) count from mb_crm_apply_data_clickhouse_map";//查询表数据量

        String address = "jdbc:clickhouse://192.168.0.141:8123/user_portrait";
        Connection connection = null;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(address, "default", "123qwe");
            exeSql(sqlDB, connection);
            exeSql(sqlTab, connection);
            exeSql(sqlCount, connection);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {//关闭连接
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void exeSql(String sql, Connection connection) {
        Statement statement = null;
        ResultSet results = null;
        try {
            statement = connection.createStatement();
            long begin = System.currentTimeMillis();
            results = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            System.out.println("执行（" + sql + "）耗时：" + (end - begin) + "ms");
            ResultSetMetaData rsmd = results.getMetaData();
            List<Map> list = new ArrayList();
            while (results.next()) {
                Map map = new HashMap();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)));
                }
                list.add(map);
            }
            for (Map map : list) {
                System.err.println(map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {//关闭连接
            try {
                if (results != null) {
                    results.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
