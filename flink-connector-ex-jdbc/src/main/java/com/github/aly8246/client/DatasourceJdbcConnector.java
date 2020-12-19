package com.github.aly8246.client;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DatasourceJdbcConnector implements JdbcConnector<Connection, Row> {

    @Override
    public Connection syncConnector(String jdbcUrl, String username, String password, String driver) {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName(driver);
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            config.setUsername(username);
            config.setPassword(password);
        }
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setAutoCommit(true);
        config.setReadOnly(false);
        config.setConnectionTestQuery("SELECT 1");
        config.setConnectionTimeout(9000);
        config.setIdleTimeout(30000);
        config.setMaxLifetime(1800000);
        config.setMaximumPoolSize(32);
        config.setMinimumIdle(5);
        config.setValidationTimeout(3000);

        try {
            return new HikariDataSource(config).getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<JdbcResult> select(String sql, Connection connectorClient) {
        try {
            PreparedStatement preparedStatement = connectorClient.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            List<JdbcResult> jdbcResultList=new LinkedList<>();
            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                Object[] objects = new Object[metaData.getColumnCount()];
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    objects[i] = resultSet.getObject(i + 1);
                }

                Row row = Row.of(objects);
                jdbcResultList.add(new JdbcResult(row,metaData));
            }
            return jdbcResultList;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }


    @Override
    public void select(String sql, BiConsumer<List<Row>, ResultSetMetaData> successHandler, Consumer<Throwable> exceptionHandler, Connection connectorClient) {
        throw new UnsupportedOperationException("hikari不支持lambda模式");
    }

    @Override
    public Connection asyncConnector(String jdbcUrl, String username, String password, String driver) {
        throw new UnsupportedOperationException("hikari暂未支持异步连接器");
    }
}
