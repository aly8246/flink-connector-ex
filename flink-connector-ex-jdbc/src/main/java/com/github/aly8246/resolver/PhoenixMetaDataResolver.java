package com.github.aly8246.resolver;

import com.github.aly8246.catalog.PhoenixTypeMapping;
import com.github.aly8246.client.JdbcConnector;
import com.github.aly8246.dialect.CatalogDialect;
import com.github.aly8246.resolver.MetaDataResolver;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * catalog表结构解析
 */
public class PhoenixMetaDataResolver implements MetaDataResolver {
    private String[] columnNames;
    private DataType[] columnDataTypes;
    private List<MetaDataResolver.CatalogEntity> catalogEntityList;

    private List<JdbcConnector.JdbcResult> jdbcResultList;

    /**
     * 从catalog数据库查询表的catalog的rows，然后解析成字段名称，数据类型
     * 要非常注意的是，这里e.row.getField的index和{@link CatalogDialect#catalogQueryStmt(org.apache.flink.table.catalog.ObjectPath)}
     * 返回sql里面select的字段顺序一致
     * 假设select 字段名，表名 from catalog
     * 那么 e.row.getField(0) 就是字段名，e.row.getField(1)就是表名
     *
     * @param jdbcResultList 从jdbc查询catalog的结构
     */
    @Override
    public void setJdbcResult(List<JdbcConnector.JdbcResult> jdbcResultList) {
        this.jdbcResultList = jdbcResultList;
        List<MetaDataResolver.CatalogEntity> catalogEntityList = jdbcResultList
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
                    return new MetaDataResolver.CatalogEntity(tableName, columnName, columnTypeId, columnDataType, columnSize, pk == 1, nullable != 0, order);
                })
                .collect(Collectors.toList());

        //获取字段名字
        this.columnNames = catalogEntityList.stream().map(MetaDataResolver.CatalogEntity::getFieldName).toArray(String[]::new);
        //获取字段数据类型
        this.columnDataTypes = catalogEntityList.stream().map(MetaDataResolver.CatalogEntity::getDataType).toArray(DataType[]::new);
        //获取catalog实体类
        this.catalogEntityList = catalogEntityList;
    }

    /**
     * 获取该表的字段名称
     */
    @Override
    public String[] getColumnNames() {
        return columnNames;
    }

    /**
     * 获取该表的字段数据类型
     */
    @Override
    public DataType[] getColumnDataTypes() {
        return columnDataTypes;
    }

    /**
     * 获取catalog结果集
     */
    @Override
    public List<MetaDataResolver.CatalogEntity> getCatalogEntityList() {
        return catalogEntityList;
    }


}
