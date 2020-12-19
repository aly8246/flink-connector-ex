package com.github.aly8246.catalog;

import com.github.aly8246.client.JdbcConnector;
import com.github.aly8246.dialect.CatalogDialect;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * catalog表结构解析
 */
public class PhoenixCatalog implements Serializable {
    private final String[] columnNames;
    private final DataType[] columnDataTypes;
    private final List<CatalogEntity> catalogEntityList;

    /**
     * 从catalog数据库查询表的catalog的rows，然后解析成字段名称，数据类型
     * 要非常注意的是，这里e.row.getField的index和{@link CatalogDialect#catalogQueryStmt(org.apache.flink.table.catalog.ObjectPath)}
     * 返回sql里面select的字段顺序一致
     * 假设select 字段名，表名 from catalog
     * 那么 e.row.getField(0) 就是字段名，e.row.getField(1)就是表名
     *
     * @param jdbcResultList 从jdbc查询catalog的结构
     */
    public PhoenixCatalog(List<JdbcConnector.JdbcResult> jdbcResultList) {

        List<CatalogEntity> catalogEntityList = jdbcResultList
                .stream()
                .filter(e -> e.row.getField(0) != null)//表名不能为空
                .filter(e -> e.row.getField(1) != null)//字段名不能为空
                .filter(e -> e.row.getField(2) != null)//字段类型不能为空
                .filter(e -> e.row.getField(4) != null)//可空选项不能为空
                .filter(e -> e.row.getField(5) != null)//字段序号不能为空
                .map(e -> {
                    //表名
                    String tableName = Objects.requireNonNull(e.row.getField(0)).toString();

                    //字段名
                    String columnName = Objects.requireNonNull(e.row.getField(1)).toString().toLowerCase();

                    //字段数据类型
                    Integer columnTypeId = (Integer) e.row.getField(2);

                    //字段大小
                    Object columnSizeObj = e.row.getField(3);
                    Integer columnSize = columnSizeObj != null ? Integer.parseInt(columnSizeObj.toString()) : 2147483647;

                    //可空选项
                    Integer nullable = (Integer) Objects.requireNonNull(e.row.getField(4));

                    //序号
                    Integer order = (Integer) e.row.getField(5);

                    //是否是主键
                    Object pkObj = e.row.getField(6);
                    short pk = pkObj != null ? Short.parseShort(pkObj.toString()) : 0;

                    //字段flink-jdbc类型
                    DataType columnDataType = PhoenixTypeMapping.fromJdbcTypeId(Objects.requireNonNull(columnTypeId), columnSize);

                    //返回catalog实体类
                    return new CatalogEntity(tableName, columnName, columnTypeId, columnDataType, columnSize, pk == 1, nullable != 0, order);
                })
                .collect(Collectors.toList());

        //获取字段名字
        this.columnNames = catalogEntityList.stream().map(CatalogEntity::getFieldName).toArray(String[]::new);
        //获取字段数据类型
        this.columnDataTypes = catalogEntityList.stream().map(CatalogEntity::getDataType).toArray(DataType[]::new);
        //获取catalog实体类
        this.catalogEntityList = catalogEntityList;
    }

    /**
     * 获取该表的字段名称
     */
    public String[] getColumnNames() {
        return columnNames;
    }

    /**
     * 获取该表的字段数据类型
     */
    public DataType[] getColumnDataTypes() {
        return columnDataTypes;
    }

    /**
     * 获取catalog结果集
     */
    public List<CatalogEntity> getCatalogEntityList() {
        return catalogEntityList;
    }

    /**
     * 用于存放phoenix catalog数据
     */
    public static class CatalogEntity implements Serializable {
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
