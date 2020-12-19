package com.github.aly8246.dialect;

import org.apache.flink.table.catalog.ObjectPath;

/**
 * java spi注册catalog方言
 */
public class PhoenixCatalogDialect implements CatalogDialect {

    @Override
    public boolean supported(String jdbcUrl) {
        return jdbcUrl.startsWith("jdbc:phoenix");
    }

    /**
     * 返回默认数据库名字
     */
    @Override
    public String defaultSchemaName() {
        return "DEFAULT";
    }

    /**
     * 返回默认驱动名字
     */
    @Override
    public String defaultDriverName() {
        return "org.apache.phoenix.jdbc.PhoenixDriver";
    }

    /**
     * 返回默认catalog名字
     */
    @Override
    public String defaultCatalogName() {
        return "phoenix";
    }

    /**
     * 从jdbc url中获取phoenix的数据库名字
     * example:
     * jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181/default
     * phoenix不同于mysql，需要单独额外解析
     *
     * @param jdbcUrl jdbc连接地址
     * @return 数据库名字
     */
    @Override
    public String getDatabaseName(String jdbcUrl) {
        //如果不包含斜杠，可能是没有指定数据库
        if (!jdbcUrl.contains("/")) return null;

        //有斜杠则拿出数据库名字
        return jdbcUrl.split("/")[1];
    }

    @Override
    public String catalogQueryStmt(ObjectPath tablePath) {
        return "select " +
                "TABLE_NAME," +
                "COLUMN_NAME," +
                "DATA_TYPE," +
                "COLUMN_SIZE," +
                "NULLABLE," +
                "ORDINAL_POSITION," +
                "KEY_SEQ " +
                "from SYSTEM.CATALOG " +
                "WHERE TABLE_NAME = '" + tablePath.getObjectName().toUpperCase() + "'";
    }
}
