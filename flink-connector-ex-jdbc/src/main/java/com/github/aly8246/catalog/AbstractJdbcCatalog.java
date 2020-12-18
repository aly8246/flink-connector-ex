package com.github.aly8246.catalog;

import com.github.aly8246.option.JdbcOption;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * 抽象类，继承AbstractCatalog 获得读取元数据的能力
 */
public abstract class AbstractJdbcCatalog extends AbstractCatalog {
    protected final JdbcOption jdbcOption;

    public AbstractJdbcCatalog(JdbcOption jdbcOption) {
        super(jdbcOption.getCatalogName(), jdbcOption.getDatabase());
        this.jdbcOption = jdbcOption;
    }


    /**
     * TODO 在此通过DriverManager测试获取连接，并不是为了查询数据，而是简单的连接数据库查看是否能连通
     * TODO 后续立即关闭连接
     */
    @Override
    public void open() throws CatalogException {

    }

}
