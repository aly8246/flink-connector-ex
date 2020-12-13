package com.github.aly8246.source;

import com.github.aly8246.option.JdbcContext;
import com.github.aly8246.option.JdbcOperation;
import com.github.aly8246.option.JdbcOption;
import com.github.aly8246.option.JdbcSourceSinkContext;
import com.github.aly8246.utils.JdbcUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JdbcSyncTableFunction extends TableFunction<Row> {
    private final JdbcContext context;
    private final String[] lookupKeys;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient Cache<Object, List<Row>> cache;


    public JdbcSyncTableFunction(JdbcContext context, String[] lookupKeys) {
        this.context = context;
        this.lookupKeys = lookupKeys;
    }

    /**
     * 打开数据库连接,初始化查询句
     */
    @Override
    public void open(FunctionContext context) {
        //创建流表的连表条件sql
        JdbcOperation jdbcOperation = new JdbcOperation(this.context);
        DataSource dataSource = jdbcOperation.openSyncConnection();
        String queryStmt = this.context.getQueryStmt(this.context.getTableSchema().getFieldNames(), this.lookupKeys);
        try {
            //连接数据库
            this.connection = dataSource.getConnection();

            //预编译sql语句
            this.statement = connection.prepareStatement(queryStmt);
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
     * 执行完毕。关闭连接
     */
    @Override
    public void close() throws Exception {
        if (this.statement != null) this.statement.close();
        if (this.connection != null) this.connection.close();
    }

    /**
     * 执行连表查询的真实方法
     *
     * @param keys 连表字段
     */
    public void eval(Object... keys) {
        //根据表字段数量创建row

        //先从缓存中查询
        if (this.cache != null) {
            List<Row> cacheRows = cache.getIfPresent(keys[0]);
            //缓存命中
            if (cacheRows != null) {
                cacheRows.forEach(this::collect);
                //结束执行
                return;
            }
        }

        try {
            //先清理预编译stmt的参数
            this.statement.clearParameters();

            //将参数设置到stmt
            JdbcUtil.setFields(this.statement, lookupKeys, keys.clone(), this.context.getTableSchema());

            //执行查询并且获取返回结果
            ResultSet resultSet = this.statement.executeQuery();

            //开始解析 resultSet
            List<Row> rowList = new ArrayList<>();
            while (resultSet.next()) {
                Row row = new Row(this.context.getTableSchema().getFieldCount());

                //把查询结果填充到row
                for (int i = 0; i < row.getArity(); i++) {
                    row.setField(i, resultSet.getObject(i + 1));
                }
                //同步返回查询结果
                collect(row);

                if (cache != null) {
                    rowList.add(row);
                }
            }

            //数据读取完毕，关闭resultSet
            resultSet.close();

            //缓存数据
            if (cache != null) {
                cache.put(keys[0], rowList);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


    /**
     * 返回字段的类型
     */
    @Override
    public TypeInformation<Row> getResultType() {
        return this.context.getTableSchema().toRowType();
    }
}
