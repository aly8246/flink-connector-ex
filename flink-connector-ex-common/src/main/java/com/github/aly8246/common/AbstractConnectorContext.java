package com.github.aly8246.common;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

public abstract class AbstractConnectorContext<T extends BaseOption> implements ConnectorContext<T> {
    //配置选项
    private final T option;

    /**
     * 原始配置文件
     */
    private final Map<String, String> properties;

    @Override
    public T getOption() {
        return this.option;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }


    /**
     * 获取schema，由于schema没有序列化，所以只能每次都创建一个新的
     * 支持创建factory的时候已经验证过this.properties
     * 所以这里直接getTableSchema来获取TableSchema
     *
     * @return TableSchema
     */
    @Override
    public TableSchema getTableSchema() {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(this.properties);
        return TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
    }


    /**
     * 获取RowTypeInfo
     * 有些时间不会select *
     * 只会select 某些字段
     *
     * @param selectFields 要查询的字段
     */
    @Override
    public TableSchema getSelectFieldsTableSchema(int[] selectFields) {
        TableSchema sourceTableSchema = this.getTableSchema();

        if (selectFields != null) {
            DataType[] selectFieldsTypes = new DataType[selectFields.length];
            String[] selectFieldsNames = new String[selectFields.length];

            //拿到要select的字段
            for (int i = 0; i < selectFields.length; i++) {
                selectFieldsTypes[i] = sourceTableSchema.getFieldDataTypes()[selectFields[i]];
                selectFieldsNames[i] = sourceTableSchema.getFieldNames()[selectFields[i]];
            }

            //创建一个根据查询字段创建的schema
            return TableSchema.builder()
                    .fields(selectFieldsNames, selectFieldsTypes)
                    .build();
        } else
            return sourceTableSchema;
    }

    /**
     * 是否是异步表
     */
    @Override
    public boolean isAsyncSupported() {
        return this.option.asyncSupported;
    }

    public AbstractConnectorContext(T option, Map<String, String> properties) {
        this.option = option;
        this.properties = properties;
    }
}
