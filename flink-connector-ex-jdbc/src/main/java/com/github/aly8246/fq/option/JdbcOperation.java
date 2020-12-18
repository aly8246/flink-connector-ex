//package com.github.aly8246.fq.option;
//
//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
//import io.vertx.core.Vertx;
//import io.vertx.core.VertxOptions;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import io.vertx.ext.jdbc.JDBCClient;
//import io.vertx.ext.sql.ResultSet;
//import io.vertx.ext.sql.SQLClient;
//import io.vertx.ext.sql.SQLConnection;
//import org.apache.flink.types.Row;
//
//import javax.sql.DataSource;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.function.Consumer;
//
//public class JdbcOperation {
//    private final JdbcContext context;
//
//    public JdbcOperation(JdbcContext context) {
//        this.context = context;
//    }
//
//
//    /**
//     * 同步连接
//     *
//     * @return
//     */
//    public DataSource openSyncConnection() {
//        JdbcOption option = this.context.getOption();
//        HikariConfig config = new HikariConfig();
//
//        config.setJdbcUrl(option.getUrl());
//        config.setDriverClassName(option.getJdbcDriver());
//        config.setUsername(option.getUsername());
//        config.setPassword(option.getPassword());
//        config.addDataSourceProperty("cachePrepStmts", "true");
//        config.addDataSourceProperty("prepStmtCacheSize", "250");
//        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
//        config.setAutoCommit(true);
//        config.setReadOnly(false);
//        config.setConnectionTestQuery("SELECT 1");
//        config.setConnectionTimeout(9000);
//        config.setIdleTimeout(30000);
//        config.setMaxLifetime(1800000);
//        config.setMaximumPoolSize(32);
//        config.setMinimumIdle(5);
//        config.setValidationTimeout(3000);
//
//        return new HikariDataSource(config);
//    }
//
//    /**
//     * 异步连接
//     *
//     * @return
//     */
//    public SQLClient openAsyncConnection() {
//        Vertx vertx = Vertx.vertx(new VertxOptions()
//                .setWorkerPoolSize(10)
//                .setEventLoopPoolSize(10));
//
//        JdbcOption jdbcOption = this.context.getOption();
//
//        JsonObject config = new JsonObject()
//                .put("jdbcUrl", jdbcOption.getUrl())
//                .put("driverClassName", jdbcOption.getJdbcDriver())
//                .put("autoCommit", true)
//                .put("connectionTimeout", 9000)
//                .put("idleTimeout", 30000)
//                .put("maxLifetime", 1800000)
//                .put("minimumIdle", 5)
//                .put("maximumPoolSize", 32)
//                .put("poolName", "flink-jdbc-async-source-pool")
//                .put("connectionTestQuery", "SELECT 1")
//                .put("provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider");
//
//        if (jdbcOption.hasPassword()) {
//            config.put("user", jdbcOption.getUsername())
//                    .put("password", jdbcOption.getPassword());
//        }
//        return JDBCClient.createShared(vertx, config);
//    }
//
//
//    /**
//     * 创建查询
//     *
//     * @param selectFields     要查询的字段
//     * @param successHandler   查询成功回调
//     * @param exceptionHandler 查询失败回调
//     */
//    public void select(String[] selectFields, Consumer<List<Row>> successHandler, Consumer<Throwable> exceptionHandler) {
//        SQLClient sqlClient = this.openAsyncConnection();
//        select(selectFields, successHandler, exceptionHandler, sqlClient);
//    }
//
//    /**
//     * 创建查询
//     *
//     * @param selectFields     要查询的字段
//     * @param successHandler   查询成功回调
//     * @param exceptionHandler 查询失败回调
//     * @param sqlClient        vertx Sql
//     */
//    public void select(String[] selectFields, Consumer<List<Row>> successHandler, Consumer<Throwable> exceptionHandler, SQLClient sqlClient) {
//        this.select(this.context.getQueryStmt(selectFields), successHandler, exceptionHandler, sqlClient);
//    }
//
//    /**
//     * 创建查询，并且返回flink rowList
//     *
//     * @param sql              要查询的sql
//     * @param successHandler   查询成功回调
//     * @param exceptionHandler 查询失败回调
//     * @param sqlClient        vertx Sql
//     */
//    public void select(String sql, Consumer<List<Row>> successHandler, Consumer<Throwable> exceptionHandler, SQLClient sqlClient) {
//        //打开sql连接
//        sqlClient.getConnection(resultHandler -> {
//            //获取sql连接
//            SQLConnection sqlConnection = resultHandler.result();
//            //查询结果
//            resultHandler.result().query(sql, resultSetAsyncResult -> {
//
//                //查询成功
//                if (resultSetAsyncResult.succeeded()) {
//                    ResultSet result = resultSetAsyncResult.result();
//
//                    //封装查询结果
//                    List<Row> rowList = new LinkedList<>();
//                    while (result != null) {
//                        for (JsonArray jsonArray : result.getResults()) {
//                            Row completeRow = Row.of(jsonArray.stream().toArray());
//                            rowList.add(completeRow);
//                        }
//                        result = result.getNext();
//                    }
//                    //成功用户回调
//                    successHandler.accept(rowList);
//                }
//
//                //查询失败
//                if (resultSetAsyncResult.failed() && exceptionHandler != null) {
//                    Throwable cause = resultSetAsyncResult.cause();
//                    exceptionHandler.accept(cause);
//                }
//            });
//            //关闭连接
//            sqlConnection.close();
//        });
//    }
//
//}
