package com.github.aly8246.client;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 抽象jdbc连接
 *
 * @param <C> client or connection
 * @param <R> result
 */
public interface JdbcConnector<C, R extends Row> extends Serializable {

    /**
     * 创建一个jdbc异步连接器
     */
    C asyncConnector(String jdbcUrl, String username, String password, String driver);

    C syncConnector(String jdbcUrl, String username, String password, String driver);

    /**
     * 创建一个jdbc连接器，并且不需要密码认证
     *
     * @param jdbcUrl jdbcUrl
     */
    default C asyncConnector(String jdbcUrl, String driver) {
        return this.asyncConnector(jdbcUrl, null, null, driver);
    }

    /**
     * 通过sql语句进行查询并且拿到返回结果
     *
     * @param sql              查询sql
     * @param successHandler   成功回调方法
     * @param exceptionHandler 异常回调方法
     * @param connectorClient  jdbc客户端
     * @return
     */
    void select(String sql, BiConsumer<List<Row>, ResultSetMetaData> successHandler, Consumer<Throwable> exceptionHandler, C connectorClient);

    List<JdbcResult> select(String sql, C connectorClient);

    class JdbcResult {
        public Row row;
        public ResultSetMetaData metaData;

        public JdbcResult(Row result, ResultSetMetaData metaData) {
            this.row = result;
            this.metaData = metaData;
        }
    }

}
