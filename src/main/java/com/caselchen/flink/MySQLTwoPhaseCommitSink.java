package com.caselchen.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * 实现两阶段提交MySQL
 */
public class MySQLTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<List<ObjectNode>, Connection, Void>  implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MySQLTwoPhaseCommitSink.class);

    private String drivername;
    private String url;
    private String username;
    private String password;
    private String sql;
    private int[] types;

    public MySQLTwoPhaseCommitSink(String url, String drivername, String username, String password, String sql, int[] types) {
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
        this.drivername = drivername;
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
        this.types = types;
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     *
     * @param connection
     * @param data
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, List<ObjectNode> data, Context context) throws Exception {
        log.info("start invoke...{},线程id: {}",connection, Thread.currentThread().getId());

        log.info("使用连接:{} 创建游标...,线程id: {}",connection, Thread.currentThread().getId());
        PreparedStatement prepareStatement = connection.prepareStatement(this.sql);
        log.info("创建 ps:{} 成功...,线程id: {}",prepareStatement.toString(), Thread.currentThread().getId());

        data.forEach(objectNode -> {
            try {
                String value = objectNode.get("value").toString();
                JSONObject valueJson = JSONObject.parseObject(value);
                prepareStatement.setObject(1, valueJson.get("id"));
                prepareStatement.setObject(2, valueJson.get("name"));
                prepareStatement.setObject(3, valueJson.get("password"));
                prepareStatement.setObject(4, valueJson.get("age"));
                prepareStatement.setObject(5, valueJson.get("salary"));
                prepareStatement.setObject(6, valueJson.get("department"));

                prepareStatement.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        log.info("start executeBatch, 使用ps:{}...,线程id: {}",prepareStatement.toString(), Thread.currentThread().getId());
        prepareStatement.executeBatch();

        log.info("准备关闭ps:{} ...,线程id: {}",prepareStatement.toString(), Thread.currentThread().getId());
        prepareStatement.close();

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("start snapshotState...,线程id: {}", Thread.currentThread().getId());
        super.snapshotState(context);
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     *
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......,线程id: {}", Thread.currentThread().getId());
        Connection connection = null;

        try {
            log.info("create connection.......");
            connection = this.establishConnection();
            log.info("建立连接:{} 成功...,线程id: {}",connection, Thread.currentThread().getId());

        } catch (SQLException var4) {
            throw new IllegalArgumentException("open() failed.", var4);
        } catch (ClassNotFoundException var5) {
            throw new IllegalArgumentException("JDBC driver class not found.", var5);
        }

        // 设置手动提交
        connection.setAutoCommit(false);

        return connection;
    }

    private Connection establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(this.drivername);
        if (this.username == null) {
            return DriverManager.getConnection(this.url);
        } else {
            return DriverManager.getConnection(this.url, this.username, this.password);
        }
    }



    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     *
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...{} ...,线程id: {}",connection, Thread.currentThread().getId());
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     *
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit..{},线程id: {}",connection, Thread.currentThread().getId());
        if (connection != null) {
            try {
                log.info("准备提交事务，使用连接：{} ...,线程id: {}",connection, Thread.currentThread().getId());
                connection.commit();

                close(connection);
            } catch (SQLException e) {
                log.error("提交事务失败,Connection: {},线程id: {}",connection, Thread.currentThread().getId());
                e.printStackTrace();
            } finally {

            }
        }
    }

    @Override
    protected void recoverAndCommit(Connection connection) {
        log.info("start recoverAndCommit {}.......,线程id: {}",connection, Thread.currentThread().getId());
    }


    @Override
    protected void recoverAndAbort(Connection connection) {
        log.info("start abort recoverAndAbort {}.......,线程id: {}",connection, Thread.currentThread().getId());
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     *
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback... {} ,线程id: {}",connection, Thread.currentThread().getId());
        if (connection != null) {
            try {
                log.error("事务发生回滚,Connection: {} ,线程id: {}",connection, Thread.currentThread().getId());
                connection.rollback();

//                close(connection);
            } catch (SQLException e) {
                log.error("事物回滚失败,Connection: {} ,线程id: {}",connection, Thread.currentThread().getId());
            } finally {
//                close(connection);
            }
        }
    }

    /**
     * 关闭连接
     *
     * @param connection
     */
    public void close(Connection connection) {
        if (connection != null) {
            try {
                log.info("准备关闭连接:{} ...,线程id: {}",connection, Thread.currentThread().getId());
                connection.close();
            } catch (SQLException var12) {
                log.info("JDBC connection could not be closed: " + var12.getMessage());
            } finally {
//                connection = null;
            }
        }
    }

}