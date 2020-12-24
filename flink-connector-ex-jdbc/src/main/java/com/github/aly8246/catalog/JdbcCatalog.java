package com.github.aly8246.catalog;

import com.github.aly8246.dialect.CatalogDialect;
import com.github.aly8246.dialect.CatalogDialectServices;
import com.github.aly8246.dialect.PhoenixCatalogDialect;
import com.github.aly8246.option.JdbcOption;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Catalogs for relational databases via JDBC.
 */
@PublicEvolving
public class JdbcCatalog extends AbstractJdbcCatalog {

    private final AbstractJdbcCatalog internal;

    public JdbcCatalog(JdbcOption jdbcOption) {
        super(jdbcOption);

        //获取支持url的catalog
        CatalogDialect<? extends Catalog> catalogDialect = jdbcOption.getCatalogDialect();

        //创建该catalog
        internal = catalogDialect.createCatalog(jdbcOption);
    }

    // ------ databases -----

    @Override
    public List<String> listDatabases() throws CatalogException {
        return internal.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        return internal.getDatabase(databaseName);
    }

    // ------ tables and views ------

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        return internal.listTables(databaseName);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return internal.getTable(tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName()) &&
                    listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    // ------ getters ------

    @VisibleForTesting
    public AbstractJdbcCatalog getInternal() {
        return internal;
    }
}
