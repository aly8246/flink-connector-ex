package com.github.aly8246.factory;

import com.github.aly8246.JdbcCatalogValidator;
import com.github.aly8246.dialect.CatalogDialect;
import com.github.aly8246.option.JdbcOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.aly8246.JdbcCatalogValidator.*;

public class PhoenixCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String s, Map<String, String> map) {
        //验证配置文件
        final DescriptorProperties prop = getValidatedProperties(map);

        //通过验证后的配置文件创建jdbc选项
        JdbcOption jdbcOption = new JdbcOption(prop);

        //获取支持url的catalog
        CatalogDialect<? extends Catalog> catalogDialect = jdbcOption.getCatalogDialect();

        //创建catalog
        return catalogDialect.createCatalog(jdbcOption);
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

    /**
     * 获取验证后的配置文件
     *
     * @param properties 原始配置文件
     * @return 验证后的配置文件
     */
    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        //验证配置文件是否支持jdbc catalog
        new JdbcCatalogValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
