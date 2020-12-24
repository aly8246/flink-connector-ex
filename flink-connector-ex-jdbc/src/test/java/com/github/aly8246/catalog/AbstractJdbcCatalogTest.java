package com.github.aly8246.catalog;

import com.github.aly8246.JdbcCatalogDescriptor;
import com.github.aly8246.option.JdbcOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class AbstractJdbcCatalogTest {

    protected static JdbcOption jdbcOption;
    protected static Catalog catalog;

    protected static final String JDBC_URL = "jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181/default";

    @BeforeClass
    public static void setup() {
        jdbcOption = new JdbcOption
                .JdbcOptionBuilder()
                .url(JDBC_URL)
                .build();
        catalog = new PhoenixJdbcCatalog(jdbcOption);
    }

    @Test
    public void test() {
        final CatalogDescriptor catalogDescriptor =
                new JdbcCatalogDescriptor(PhoenixJdbcCatalog.DEFAULT_DATABASE, null, null, JDBC_URL);

        final Map<String, String> properties = catalogDescriptor.toProperties();

        final Catalog actualCatalog = TableFactoryService.find(CatalogFactory.class, properties)
                .createCatalog("aa", properties);

        System.out.println(actualCatalog);
    }
}
