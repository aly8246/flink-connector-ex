package com.github.aly8246.client;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

import java.sql.ResultSetMetaData;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 使用vert.x来创建SQLClient
 */
public class VertxAsyncJdbcConnector implements JdbcConnector<SQLClient, Row> {

    /**
     * 创建vert.x 异步mysql客户端
     *
     * @param jdbcUrl  jdbc连接信息
     * @param username 数据库用户名
     * @param password 数据库密码
     * @return jdbc异步客户端
     */
    @Override
    public SQLClient asyncConnector(String jdbcUrl, String username, String password, String driver) {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setWorkerPoolSize(10)
                .setEventLoopPoolSize(10));
        JsonObject config = new JsonObject()
                .put("jdbcUrl", jdbcUrl)
                .put("driverClassName", driver)
                .put("autoCommit", true)
                .put("connectionTimeout", 9000)
                .put("idleTimeout", 30000)
                .put("maxLifetime", 1800000)
                .put("minimumIdle", 5)
                .put("maximumPoolSize", 32)
                .put("poolName", "flink-jdbc-async-source-pool")
                .put("connectionTestQuery", "SELECT 1")
                .put("provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider");

        //用户名和密码都不为空的时候才添加到vertx的配置里去
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            config.put("user", username)
                    .put("password", password);
        }
        return JDBCClient.createShared(vertx, config);
    }

    @Override
    public SQLClient syncConnector(String jdbcUrl, String username, String password, String driver) {
        throw new UnsupportedOperationException("vert.x暂未支持同步连接器");
    }

    @Override
    public void select(String sql, BiConsumer<List<Row>, ResultSetMetaData> successHandler, Consumer<Throwable> exceptionHandler, SQLClient connectorClient) {
        //打开jdbc连接
        connectorClient.getConnection(sqlConnectionAsyncResult -> {
            //获取sql连接
            SQLConnection sqlConnection = sqlConnectionAsyncResult.result();
            //开始查询
            sqlConnection.query(sql, resultSetAsyncResult -> {
                //假如查询成功
                if (resultSetAsyncResult.succeeded()) {
                    //从查询结果中获取结果集
                    ResultSet result = resultSetAsyncResult.result();

                    //用来存放要返回的结果集，这里用linkedList来保证row的顺序性
                    List<Row> rowList = new LinkedList<>();

                    while (result != null) {
                        for (JsonArray jsonArray : result.getResults()) {
                            //创建flink row
                            Row completeRow = Row.of(jsonArray.stream().toArray());
                            rowList.add(completeRow);
                        }
                        result = result.getNext();
                    }
                    //处理完所有的row，交给用户处理
                    successHandler.accept(rowList, null);
                }
            });
            //关闭连接
            sqlConnection.close();
        });
    }

    @Override
    public List<JdbcResult> select(String sql, SQLClient connectorClient) {
        throw new UnsupportedOperationException("不支持此方法");
    }
}
