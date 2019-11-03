package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class MySqlAsyncIOJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> inputStream = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        });
        DataStream<Integer> outputStream = AsyncDataStream.unorderedWait(inputStream, new MySqlAsyncFunc(),
                6000, TimeUnit.MILLISECONDS, 100);
        outputStream.print();
        env.execute("MySqlAsyncIOJob");
    }

    static class MySqlAsyncFunc extends RichAsyncFunction<Integer, Integer> {

        private Connection con = null;
        private PreparedStatement statement = null;
        private ResultSet rs = null;

        public MySqlAsyncFunc() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "Chenshuai@123");
        }

        @Override
        public void close() throws Exception {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (con != null) {
                con.close();
            }
        }

        @Override
        public void asyncInvoke(Integer value, ResultFuture<Integer> resultFuture) throws Exception {
            String sql = "select aid from ads where tid = ?";
            statement = con.prepareStatement(sql);
            statement.setInt(1, value);
            rs = statement.executeQuery();
            while (rs.next()) {
                int aid = rs.getInt("aid");
                resultFuture.complete(Collections.singleton(aid));
            }
        }
    }
}
