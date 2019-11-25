package com.caselchen.flink;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.sql.*;
import java.util.Properties;

public class OrientdbJDBCExample {
    public static void main(String[] args) throws Exception {
        Class.forName("com.orientechnologies.orient.jdbc.OrientJdbcDriver");

        Properties info = new Properties();
        info.put("user", "root");
        info.put("password", "changeit");
        info.put("db.usePool", "true"); // USE THE POOL
        info.put("db.pool.min", "3");   // MINIMUM POOL SIZE

        Connection conn = DriverManager.getConnection("jdbc:orient:remote:localhost/demodb", info);

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT Id, Name FROM Countries");
        while(rs.next()) {
            Long id = rs.getLong("Id");
            String name = rs.getString("Name");
            System.out.println(new Countries(id, name));
        }

//        PreparedStatement ps = conn.prepareStatement("INSERT INTO Countries (Id, Name) VALUES (?, ?)");
//        PreparedStatement ps = conn.prepareStatement("UPDATE Countries SET Name = ? UPSERT WHERE Id = ?");
//        ps.setObject(1, "WEB");
//        ps.setObject(2, 10001L);
//        ps.execute();

//        ps.addBatch();
//
//        ps.setLong(1, 102L);
//        ps.setString(2, "WEB");
//        ps.addBatch();

//        int[] counts = ps.executeBatch();
        conn.commit();
        System.out.println("Done");
//        ps.close();

        rs.close();
        stmt.close();
        conn.close();
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class Countries implements Serializable {
        private Long id;
        private String name;
    }
}
