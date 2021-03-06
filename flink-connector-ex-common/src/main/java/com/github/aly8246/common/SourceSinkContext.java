package com.github.aly8246.common;

import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.Map;

public interface SourceSinkContext<T extends BaseOption> extends Serializable {

    /**
     * 获取配置选项
     */
    public T getOption();

    /**
     * 获取link-sql ddl相关配置
     */
    public Map<String, String> getProperties();

    /**
     * 获取表信息
     * 这里每次都是重新解析生成，因为tableSchema不支持序列化
     */
    public TableSchema getTableSchema();

    /**
     * 根据查询的字段生成表信息
     */
    public TableSchema getSelectFieldsTableSchema(int[] selectFields);

    /**
     * 是否支持异步读写
     */
    public default boolean isAsyncSupported() {
        return this.getOption().asyncSupported;
    }

    /**
     * 异步流超时时间 默认要比buffer时间长，否则buffer里还没flush到db，异步流就超时了
     */
    public default Long asyncStreamWaitTime() {
        return this.getOption().getBufferMaxInterval() / 2 + this.getOption().getBufferMaxInterval();
    }

}
