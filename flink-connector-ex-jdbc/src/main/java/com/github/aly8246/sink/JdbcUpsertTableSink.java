package com.github.aly8246.sink;

import com.github.aly8246.option.JdbcContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 异步时间默认3秒
 * 异步批量写入时间 假设为10秒
 * 异步等待时间必须要15秒
 * 批量次数默认1次
 */
public class JdbcUpsertTableSink implements UpsertStreamTableSink<Row> {
    private final JdbcContext context;
    private String[] keyfields;
    private Boolean isAppendOnly;

    public JdbcUpsertTableSink(JdbcContext context) {
        this.context = context;
    }

    /**
     * 消费dataStream
     * 将同步流转为异步流
     * 然后使用异步消费
     * 使用printSink到标准输出流
     */
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return AsyncDataStream
                .unorderedWait(dataStream, new TestAsyncSinkFunction(), this.context.asyncStreamWaitTime(), TimeUnit.MILLISECONDS)
                .print("asyncTableSinkResult")
                .setParallelism(dataStream.getParallelism())
                ;
    }

    /**
     * 暂未触发
     */
    @Override
    public void setKeyFields(String[] strings) {
        this.keyfields = strings;
        System.out.println(Arrays.toString(strings));
    }

    /**
     * 是否是AppendOnly
     */
    @Override
    public void setIsAppendOnly(Boolean aBoolean) {
        this.isAppendOnly = aBoolean;
        System.out.println("isAppendOnly:" + aBoolean);
    }

    /**
     * 暂时没有出发国
     */
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        System.out.println("JdbcUpsertTableSink.configure");
        System.out.println("fieldNames = " + Arrays.deepToString(fieldNames) + ", fieldTypes = " + Arrays.deepToString(fieldTypes));
        return new JdbcUpsertTableSink(this.context);
    }

    /**
     * 获取表信息
     */
    @Override
    public TableSchema getTableSchema() {
        return this.context.getTableSchema();
    }

    /**
     * 发送dataStream 其实就是送去消费
     *
     * @param dataStream 要消费的ds
     */
    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        this.consumeDataStream(dataStream);
    }

    /**
     * 获取输出类型
     */
    @Override
    public TypeInformation<Row> getRecordType() {
        return this.context.getTableSchema().toRowType();
    }

    /**
     * 获取输出类型
     */
    @Override
    public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
    }

}
