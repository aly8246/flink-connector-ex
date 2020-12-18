package com.github.aly8246.client;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

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
    public SQLClient createConnector(String jdbcUrl, String username, String password) {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setWorkerPoolSize(10)
                .setEventLoopPoolSize(10));
        JsonObject config = new JsonObject()
                .put("jdbcUrl", jdbcUrl)
                .put("driverClassName", this.defaultJdbcDriver(jdbcUrl))
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
}
