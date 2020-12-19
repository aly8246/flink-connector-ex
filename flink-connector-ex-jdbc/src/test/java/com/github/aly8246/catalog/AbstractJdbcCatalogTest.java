package com.github.aly8246.catalog;

import com.github.aly8246.option.JdbcOption;
import org.junit.BeforeClass;
import org.junit.Test;

public class AbstractJdbcCatalogTest {

    protected static JdbcOption jdbcOption;

    protected static final String JDBC_URL = "jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181/default";

    @BeforeClass
    public static void setup() {
        jdbcOption = new JdbcOption
                .JdbcOptionBuilder()
                .url(JDBC_URL)
                .build();
    }

    @Test
    public void test() {

    }

}
