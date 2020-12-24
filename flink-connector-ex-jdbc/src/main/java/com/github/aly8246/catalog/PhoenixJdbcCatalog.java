package com.github.aly8246.catalog;

import com.github.aly8246.client.DatasourceJdbcConnector;
import com.github.aly8246.client.JdbcConnector;
import com.github.aly8246.dialect.CatalogDialect;
import com.github.aly8246.dialect.PhoenixCatalogDialect;
import com.github.aly8246.factory.JdbcDynamicTableFactory;
import com.github.aly8246.option.JdbcOption;
import com.github.aly8246.resolver.MetaDataResolver;
import com.github.aly8246.resolver.PhoenixMetaDataResolver;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.aly8246.factory.JdbcDynamicTableFactory.TABLE_NAME;
import static com.github.aly8246.factory.JdbcDynamicTableFactory.URL;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

@Internal
public class PhoenixJdbcCatalog extends AbstractJdbcCatalog {
    public static final String DEFAULT_DATABASE = "default";
    protected JdbcConnector<Connection, Row> syncJdbcConnector;
    protected transient Connection syncHikariConnection;
    private final CatalogDialect<PhoenixJdbcCatalog> catalogDialect;


    public PhoenixJdbcCatalog(JdbcOption jdbcOption) {
        super(jdbcOption);
        this.catalogDialect = new PhoenixCatalogDialect();
    }

    @Override
    public void open() throws CatalogException {
        super.open();
        //创建同步连接器
        this.syncJdbcConnector = new DatasourceJdbcConnector();

        //打开jdbc连接
        this.syncHikariConnection = syncJdbcConnector.syncConnector(this.jdbcOption.getUrl(),
                this.jdbcOption.getUsername(),
                this.jdbcOption.getPassword(),
                this.jdbcOption.getDriver());
    }

    @Override
    public void close() throws CatalogException {
        //关闭jdbc连接
        try {
            this.syncHikariConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    /**
     * {@link CatalogDatabaseImpl}
     *
     * @param databaseName
     * @return
     * @throws DatabaseNotExistException
     * @throws CatalogException
     */
    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        return new CatalogDatabaseImpl(Collections.emptyMap(), null);
    }

    /**
     * TODO 数据库是否存在，查询catalog来获取信息
     *
     * @param databaseName 数据库名字
     */
    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {

    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {

    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    /**
     * 通过ObjectPath查询表结构并且返回
     *
     * @param tablePath 表名全路径
     * @return 表结构
     */
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        //获取数据库信息和表信息，假如没有schema，就使用默认数据库名字来代替
        ObjectPath objectPath = this.catalogDialect.getOrDefault(tablePath);

        //通过jdbc查询，获取查询结果，查询表的catalog结构
        List<JdbcConnector.JdbcResult> jdbcResultList = syncJdbcConnector.select(
                this.catalogDialect.catalogQueryStmt(tablePath),
                this.syncHikariConnection);

        //把查询结果转换成catalog实体类
        MetaDataResolver metaDataResolver = new PhoenixMetaDataResolver();
        metaDataResolver.setJdbcResult(jdbcResultList);

        //根据catalog实体类来构建表结构
        TableSchema tableSchema = TableSchema
                .builder()
                .fields(metaDataResolver.getColumnNames(), metaDataResolver.getColumnDataTypes())
                .build();

        //生成配置文件，这将会传递给TableFactory，接入传统Factory模式
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR.key(), JdbcDynamicTableFactory.IDENTIFIER);
        props.put(URL.key(), this.jdbcOption.getUrl());
        props.put(TABLE_NAME.key(), objectPath.getFullName());
//        props.put(USERNAME.key(), "username");
//        props.put(PASSWORD.key(), pwd);

        return new CatalogTableImpl(tableSchema, props, null);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return false;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {

    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }


}
