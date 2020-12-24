package com.github.aly8246;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class JdbcCatalogValidator extends CatalogDescriptorValidator {
    public static final String CATALOG_TYPE_VALUE_JDBC = "jdbc-ex";
    public static final String CATALOG_JDBC_USERNAME = "username";
    public static final String CATALOG_JDBC_PASSWORD = "password";
    public static final String CATALOG_JDBC_BASE_URL = "base-url";
    public static final String CATALOG_JDBC_DRIVER = "driver";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC, false);
        properties.validateString(CATALOG_JDBC_BASE_URL, false, 1);
        properties.validateString(CATALOG_JDBC_USERNAME, true, 1);
        properties.validateString(CATALOG_JDBC_PASSWORD, true, 1);
        properties.validateString(CATALOG_DEFAULT_DATABASE, false, 1);
    }
}
