package com.github.aly8246.dialect;

import com.github.aly8246.catalog.AbstractJdbcCatalog;
import com.github.aly8246.option.JdbcOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;

import java.io.Serializable;

public interface CatalogDialect<T> extends Serializable {

    /**
     * 是否是支持的jdbcUrl
     */
    boolean supported(String jdbcUrl);

    /**
     * 默认数据库的名字
     */
    String defaultSchemaName();

    /**
     * 默认驱动类路径
     */
    String defaultDriverName();

    /**
     * 获取默认catalog名称，如果用户没有指定，则以此为主
     */
    String defaultCatalogName();

    /**
     * 获取数据库名称，假如用户没有显式指定，应当从url里解析
     * jdbc:mysql://127.0.0.1:3306/my_db
     * 应当返回my_db
     *
     * @param jdbcUrl jdbc连接地址
     * @return my_db
     */
    String getDatabaseName(String jdbcUrl);


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


    /**
     * 封装查询catalog的sql
     */
    String catalogQueryStmt(ObjectPath tablePath);

    /**
     * 创建catalog
     *
     * @param jdbcOption jdbc配置选项
     * @return 创建好的catalog
     */
    AbstractJdbcCatalog createCatalog(JdbcOption jdbcOption);
}
