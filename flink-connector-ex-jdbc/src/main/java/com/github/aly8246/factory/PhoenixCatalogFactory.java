package com.github.aly8246.factory;

import com.github.aly8246.JdbcCatalogValidator;
import com.github.aly8246.catalog.JdbcCatalog;
import com.github.aly8246.option.JdbcOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.aly8246.JdbcCatalogValidator.CATALOG_DEFAULT_DATABASE;
import static com.github.aly8246.JdbcCatalogValidator.CATALOG_PROPERTY_VERSION;
import static com.github.aly8246.JdbcCatalogValidator.CATALOG_TYPE;
import static com.github.aly8246.JdbcCatalogValidator.CATALOG_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_BASE_URL;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_USERNAME;

public class PhoenixCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String s, Map<String, String> map) {
        //验证配置文件
        final DescriptorProperties prop = getValidatedProperties(map);

        //创建jdbc参数实体
        JdbcOption.JdbcOptionBuilder jdbcOptionBuilder = new JdbcOption.JdbcOptionBuilder();

        //假如有用户名和密码就设置进去
        prop.getOptionalString(CATALOG_JDBC_USERNAME).ifPresent(jdbcOptionBuilder::username);
        prop.getOptionalString(CATALOG_JDBC_PASSWORD).ifPresent(jdbcOptionBuilder::password);

        //根据catalog名字和jdbc url来构建
        JdbcOption jdbcOption = jdbcOptionBuilder
                .url(prop.getString(CATALOG_JDBC_BASE_URL))
                .catalogName(s)
                .build();

        //创建通用catalog
        return new JdbcCatalog(jdbcOption);
    }

    /**
     * 支持这个catalog吗
     */
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC); // jdbc
        context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    /***
     * 支持的配置文件
     */
    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // default database
        properties.add(CATALOG_DEFAULT_DATABASE);

        properties.add(CATALOG_JDBC_BASE_URL);
        properties.add(CATALOG_JDBC_USERNAME);
        properties.add(CATALOG_JDBC_PASSWORD);

        return properties;
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new JdbcCatalogValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
