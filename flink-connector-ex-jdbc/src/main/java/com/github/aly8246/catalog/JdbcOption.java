package com.github.aly8246.catalog;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;

public class JdbcOption {
    //jdbc-url
    private String url;
    //jdbc驱动
    private String driver;
    //用户名
    private String username;
    //密码
    private String password;
    //
    private String database;

    private String catalogName;

    public String getUrl() {
        return url;
    }

    public String getDriver() {
        return driver;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getCatalogName() {
        return catalogName;
    }

    private JdbcOption(String url, String driver, String username, String password, String catalogName) {
        this.url = url;
        this.driver = driver;
        this.username = username;
        this.password = password;
        this.catalogName = catalogName;
    }

    public static class JdbcOptionBuilder {
        private String url;
        private String driver;
        private String username;
        private String password;
        private String catalogName;

        /**
         * 设置jdbc-url
         * example
         * jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181
         *
         * @param url jdbc-url
         * @return this
         */
        public JdbcOptionBuilder url(String url) {
            this.url = url;
            return this;
        }

        /**
         * jdbc驱动程序，通常来说都内置了默认驱动程序，一般不需要主动 设置驱动程序
         *
         * @param driver jdbc驱动程序
         * @return this
         */
        public JdbcOptionBuilder driver(String driver) {
            Preconditions.checkArgument(
                    StringUtils.isNotEmpty(driver),
                    "驱动程序不能为空"
            );
            this.driver = driver;
            return this;
        }

        /**
         * 数据库用户名和密码
         *
         * @param username 用户名
         * @param password 密码
         * @return this
         */
        public JdbcOptionBuilder usernameAndPassword(String username, String password) {
            Preconditions.checkArgument(
                    StringUtils.isNotEmpty(username) &&
                            StringUtils.isNotEmpty(password),
                    "用户名和密码不能为空"
            );
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * catalog名字，默认为catalog实现类
         *
         * @param catalogName catalog名字
         * @return this
         */
        public JdbcOptionBuilder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }


        /**
         * 懂得都懂
         *
         * @return jdbc配置选项
         */
        public JdbcOption build() {
            Preconditions.checkArgument(
                    StringUtils.isNotEmpty(url),
                    "jdbc-url不能为空"
            );
            return new JdbcOption(this.url, this.driver, this.username, this.password, this.catalogName);
        }
    }
}
