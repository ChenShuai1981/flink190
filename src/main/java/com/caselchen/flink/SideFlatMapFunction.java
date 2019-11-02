package com.caselchen.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 在维表关联中定时全量加载是针对维表数据量较少并且业务对维表数据变化的敏感程度较低的情况下可采取的一种策略，
 * 对于这种方案使用有几点需要注意：
 *
 * 全量加载有可能会比较耗时，所以必须是一个异步加载过程
 *
 * 内存维表数据需要被流表数据关联读取、也需要被定时重新加载，这两个过程是不同线程执行，
 * 为了尽可能保证数据一致性，可使用原子引用变量包装内存维表数据对象即AtomicReference
 *
 * 查内存维表数据非异步io过程
 *
 * 具体实例：广告流量统计，广告流量数据包含：广告位id,用户设备id,事件类型(点击、浏览)，发生时间，
 * 现在需要统计每个广告主在每一个时间段内的点击、浏览数量，流量数据中只有广告位id,
 * 广告位id与广告主id对应的关系在mysql 中，这是一个典型的流表关联维表过程，
 * 需要从mysql中获取该广告位id对应的广告主id, 然后在来统计。
 */
public class SideFlatMapFunction extends RichFlatMapFunction<AdData, AdData> {

    private AtomicReference<Map<Integer, Integer>> sideInfo = null;

    @Override
    public void open(Configuration parameters) {
        sideInfo = new AtomicReference<>();
        sideInfo.set(loadData());
        ScheduledExecutorService executors = Executors.newSingleThreadScheduledExecutor();
        executors.scheduleAtFixedRate(() -> reload(),5,30, TimeUnit.SECONDS);
    }

    @Override
    public void flatMap(AdData value, Collector<AdData> out) throws Exception {
        int tid = value.getTId();
        int aid = sideInfo.get().get(tid);
        AdData newV = new AdData(aid,value.getTId(),value.getClientId(),value.getActionType(),value.getTime());
        out.collect(newV);
    }

    private void reload() {
        try{
            System.out.println("do reload~");
            Map<Integer, Integer> newData = loadData();
            sideInfo.set(newData);
            System.out.println("reload ok~");
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    private Map<Integer, Integer> loadData() {
        Map<Integer, Integer> data = new HashMap<>();
        Connection con = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "Chenshuai@123");
            String sql = "select aid, tid from ads";
            statement = con.prepareStatement(sql);
            rs = statement.executeQuery();
            while (rs.next()) {
                int aid = rs.getInt("aid");
                int tid = rs.getInt("tid");
                data.put(tid, aid);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return data;
    }
}
