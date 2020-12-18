package com.github.aly8246.catalog;

import org.apache.flink.table.catalog.ObjectPath;

public interface CatalogDialect {
    /**
     * 默认数据库的名字
     */
    String defaultSchemaName();

    /**
     * 默认驱动类路径
     */
    String defaultDriverName();


    /**
     * 获取数据库名字和表名字，如果没有数据库名字，则用默认值来替代
     *
     * @param schemaAndTable objectPath
     */
    default ObjectPath getOrDefault(ObjectPath schemaAndTable) {
        //从catalog实现类获取默认schema名称
        String defaultSchemaName = this.defaultSchemaName();

        //假如包含default，则flink认为这是默认schema
        //default.table      flink默认
        //↓
        //DEFAULT.table      catalog实现类默认表名
        if (schemaAndTable.getDatabaseName().contains("default")) {
            return new ObjectPath(defaultSchemaName, schemaAndTable.getObjectName());
        }

        //假如全名里没有包含点符号.  说明没有表名，需要带上默认表名
        if (!schemaAndTable.getFullName().contains("\\.")) {
            return new ObjectPath(defaultSchemaName, schemaAndTable.getObjectName());
        }

        //走到这里证明是自定义schema，无需其他处理
        return schemaAndTable;
    }

}
