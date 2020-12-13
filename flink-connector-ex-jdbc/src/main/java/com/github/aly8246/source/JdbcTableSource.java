package com.github.aly8246.source;

import com.github.aly8246.format.input.JdbcInputFormat;
import com.github.aly8246.option.JdbcContext;
import com.github.aly8246.option.JdbcSourceSinkContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.stream.IntStream;

public class JdbcTableSource implements StreamTableSource<Row>, ProjectableTableSource<Row>, LookupableTableSource<Row> {
    private final JdbcContext context;
    private final TableSchema tableSchema;

    /**
     * 根据上下文对象和要查询的字段来创建source
     *
     * @param context      上下文对象
     * @param selectFields 要查询的字段下标
     */
    public JdbcTableSource(JdbcContext context, int[] selectFields) {
        this.context = context;
        this.tableSchema = context.getSelectFieldsTableSchema(selectFields);
    }

    /**
     * 仅根据上下文对象创建source，查询所有字段
     *
     * @param context 上下文对象
     */
    public JdbcTableSource(JdbcContext context) {
        this(context, IntStream.range(0, context.getTableSchema().getFieldCount()).toArray());
    }

    /**
     * 同步join表支持
     *
     * @param lookupKeys join字段
     */
    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return new JdbcSyncTableFunction(this.context, lookupKeys);
    }

    /**
     * 异步join表支持
     *
     * @param lookupKeys join表字段
     */
    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return new JdbcAsyncTableFunction(this.context, lookupKeys);
    }

    /**
     * 是否开启异步表支持
     */
    @Override
    public boolean isAsyncEnabled() {
        return this.context.isAsyncSupported();
    }

    /**
     * 返回row的数据类型
     */
    @Override
    public DataType getProducedDataType() {
        return this.tableSchema.toRowDataType();
    }

    /**
     * 有些时候只select几个字段，而不是全部
     * 这里创建一个争对具体字段select的tableSource
     *
     * @param fields 要查询字段的下标
     */
    @Override
    public TableSource<Row> projectFields(int[] fields) {
        return new JdbcTableSource(context, fields);
    }

    /**
     * 获取dataStream流
     * 或者用于创建流表，此时支持一次性读取的流表和动态读取的流表
     *
     * @param streamExecutionEnvironment 流处理环境
     */
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return streamExecutionEnvironment
                .createInput(new JdbcInputFormat(this.context), this.tableSchema.toRowType())
                .name(explainSource());
    }

    /**
     * flink获取schema
     */
    @Override
    public TableSchema getTableSchema() {
        return this.tableSchema;
    }

    /**
     * 获取执行计划
     */
    @Override
    public String explainSource() {
        return TableConnectorUtils.generateRuntimeName(getClass(), this.tableSchema.getFieldNames());
    }
}
