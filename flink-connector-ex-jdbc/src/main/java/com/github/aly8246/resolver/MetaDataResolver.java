package com.github.aly8246.resolver;

import com.github.aly8246.client.JdbcConnector;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.List;

public interface MetaDataResolver extends Serializable {
    void setJdbcResult(List<JdbcConnector.JdbcResult> jdbcResultList);

    String[] getColumnNames();

    DataType[] getColumnDataTypes();

    List<CatalogEntity> getCatalogEntityList();


    /**
     * 用于存放phoenix catalog数据
     */
    class CatalogEntity implements Serializable {
        private final String tableName;
        private final String fieldName;
        private final Integer typeId;
        private final DataType dataType;
        private final Integer columnSize;
        private final Boolean isPk;
        private final Boolean nullable;
        private final Integer order;

        public CatalogEntity(String tableName, String fieldName, Integer typeId, DataType dataType, Integer columnSize, Boolean isPk, Boolean nullable, Integer order) {
            this.tableName = tableName;
            this.fieldName = fieldName;
            this.typeId = typeId;
            this.dataType = dataType;
            this.columnSize = columnSize;
            this.isPk = isPk;
            this.nullable = nullable;
            this.order = order;
        }

        /**
         * 字段名称
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * 字段类型id
         */
        public Integer getTypeId() {
            return typeId;
        }

        /**
         * flink类型
         */
        public DataType getDataType() {
            return dataType;
        }

        /**
         * 是否是主键
         */
        public Boolean getPk() {
            return isPk;
        }

        /**
         * 是否可以为空
         */
        public Boolean getNullable() {
            return nullable;
        }

        /**
         * 字段序号
         */
        public Integer getOrder() {
            return order;
        }

        /**
         * 获取表名称
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * 获取字段大小
         * 假如字段大小为空，则说明是不限制长度的最大
         */
        public Integer getColumnSize() {
            return columnSize;
        }

        @Override
        public String toString() {
            return "CatalogEntity{" +
                    "tableName='" + tableName + '\'' +
                    ", fieldName='" + fieldName + '\'' +
                    ", typeId=" + typeId +
                    ", dataType=" + dataType +
                    ", columnSize=" + columnSize +
                    ", isPk=" + isPk +
                    ", nullable=" + nullable +
                    ", order=" + order +
                    '}';
        }
    }

}
