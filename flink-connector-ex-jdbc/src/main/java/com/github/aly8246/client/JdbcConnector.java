package com.github.aly8246.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

/**
 * 抽象jdbc连接
 *
 * @param <C> client or connection
 * @param <R> result
 */
public interface JdbcConnector<C, R extends Row> {

    /**
     * 接受该jdbc url吗
     */
    default Boolean acceptUrl(String jdbcUrl) {
        return jdbcUrl.startsWith("jdbc:");
    }

    /**
     * 创建一个jdbc连接器
     */
    C createConnector(String jdbcUrl, String username, String password);

    /**
     * 创建一个jdbc连接器，并且不需要密码认证
     *
     * @param jdbcUrl jdbcUrl
     */
    default C createConnector(String jdbcUrl) {
        return this.createConnector(jdbcUrl, null, null);
    }

    /**
     * 获取默认的驱动class
     *
     * @param jdbcUrl url
     */
    default String defaultJdbcDriver(String jdbcUrl) {
        if (StringUtils.isEmpty(jdbcUrl)) {
            throw new NullPointerException("jdbc url 不能为空");
        }
        if (jdbcUrl.startsWith("jdbc:mysql:"))
            return "com.mysql.cj.jdbc.Driver";
        if (jdbcUrl.startsWith("jdbc:phoenix:"))
            return "org.apache.phoenix.jdbc.PhoenixDriver";
        //没有支持的url
        throw new UnsupportedOperationException("不支持的jdbc驱动类型");
    }
}
