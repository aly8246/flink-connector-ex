package com.github.aly8246.source;

import com.github.aly8246.format.SqlPreparedStatement;
import com.github.aly8246.option.JdbcContext;
import com.github.aly8246.option.JdbcOperation;
import com.github.aly8246.option.JdbcOption;
import com.github.aly8246.utils.JdbcUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class JdbcAsyncTableFunction extends AsyncTableFunction<Row> {
    private final JdbcContext context;
    private final String[] lookupKeys;

    private transient SqlPreparedStatement statement;
    private transient Cache<Object, List<Row>> cache;
    private transient SQLClient sqlClient;
    private transient JdbcOperation jdbcOperation;

    public JdbcAsyncTableFunction(JdbcContext context, String[] lookupKeys) {
        this.context = context;
        this.lookupKeys = lookupKeys;
    }

    /**
     * 开启连接
     */
    @Override
    public void open(FunctionContext context) {
        JdbcOperation jdbcOperation = new JdbcOperation(this.context);
        this.sqlClient = jdbcOperation.openAsyncConnection();
        this.jdbcOperation = jdbcOperation;
        //创建流表的连表条件sql
        String queryStmt = this.context.getQueryStmt(this.context.getTableSchema().getFieldNames(), this.lookupKeys);

        //连接数据库
        try {
            //预编译sql语句
            this.statement = new SqlPreparedStatement(queryStmt);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        JdbcOption jdbcOption = this.context.getOption();

        //创建缓存
        this.cache = jdbcOption.getCacheRows() == -1 || jdbcOption.getCacheTtl() == -1 ? null : CacheBuilder.newBuilder()
                .expireAfterWrite(jdbcOption.getCacheTtl(), TimeUnit.MILLISECONDS)
                .maximumSize(jdbcOption.getCacheRows())
                .build();
    }

    /**
     * 执行连表查询的真实方法
     *
     * @param keys 连表字段
     */
    public void eval(CompletableFuture<List<Row>> future, Object... keys) {
        //先从缓存中查询
        if (this.cache != null) {
            //如果命中缓存，则直接complete
            Optional<List<Row>> cachedRowList = Optional.ofNullable(cache.getIfPresent(keys[0]));
            if (cachedRowList.isPresent()) {
                future.complete(cachedRowList.get());
                return;
            }
        }

        //先清理预编译stmt的参数
        this.statement.clearParameters();

        //将参数设置到stmt
        JdbcUtil.setFields(this.statement, lookupKeys, keys.clone(), this.context.getTableSchema());

        //异步查询数据
        this.jdbcOperation.select(statement.getQueryString(), resultSet -> {
            future.complete(resultSet);
            if (this.cache != null) {
                this.cache.put(keys[0], resultSet);
            }
        }, null, this.sqlClient);
    }

    /**
     * 关闭连接
     */
    @Override
    public void close() {
    }


    /**
     * 获取 结果集字段类型
     */
    @Override
    public TypeInformation<Row> getResultType() {
        return this.context.getTableSchema().toRowType();
    }

}
