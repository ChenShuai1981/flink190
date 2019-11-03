package com.caselchen.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class CustomizeAsyncFunctionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);
        DataStream<String> outputStream = AsyncDataStream.unorderedWait(inputStream, new ExecSideFunction(),
                6000, TimeUnit.MILLISECONDS, 100);
        outputStream.print();
        env.execute("CustomizeAsyncFunctionJob");
    }

    private static class ExecSideFunction extends RichAsyncFunction<String, String> {

        private Executor executors = null;
        private String sqlTemplate = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            executors = new ThreadPoolExecutor(10, 10, 0,
                    TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
            sqlTemplate = "select value from tbl1 where id=?";
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        Connection con = ConnectionFactory.getConnection();
                        String sql = sqlTemplate.replace("?", parseKey(input));
                        PreparedStatement ps = con.prepareStatement(sqlTemplate);
                        ps.setString(0, input);
                        ResultSet rs = ps.executeQuery();
                        List<String> res = new ArrayList<>();
                        while (rs.next()) {
                            String v = rs.getString("value");
                            res.add(fillData(input, v));
                        }
                        resultFuture.complete(res);
                        con.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

        }

        private String parseKey(String input) {
            return input;
        }

        private String fillData(String input, String value) {
            return input + "|" + value;
        }
    }

}
