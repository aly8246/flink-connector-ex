package com.github.aly8246;

import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static com.github.aly8246.JdbcCatalogValidator.*;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.util.Preconditions.checkArgument;

public class JdbcCatalogDescriptor extends CatalogDescriptor {
    private final String defaultDatabase;
    private final String username;
    private final String pwd;
    private final String baseUrl;

    public JdbcCatalogDescriptor(String defaultDatabase, String username, String pwd, String baseUrl) {
        super(CATALOG_TYPE_VALUE_JDBC, 1);
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(baseUrl));

        this.defaultDatabase = defaultDatabase;
        this.username = username;
        this.pwd = pwd;
        this.baseUrl = baseUrl;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putString(CATALOG_DEFAULT_DATABASE, defaultDatabase);
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(this.username) &&
                org.apache.commons.lang3.StringUtils.isNotEmpty(this.pwd)) {
            properties.putString(CATALOG_JDBC_USERNAME, username);
            properties.putString(CATALOG_JDBC_PASSWORD, pwd);
        }
        properties.putString(CATALOG_JDBC_BASE_URL, baseUrl);
        return properties.asMap();
    }
}
