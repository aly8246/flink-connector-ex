package com.github.aly8246.option;

import com.github.aly8246.common.BaseOption;
import com.github.aly8246.dialect.CatalogDialect;
import com.github.aly8246.dialect.CatalogDialectServices;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import static com.github.aly8246.JdbcCatalogValidator.*;

public class JdbcOption extends BaseOption {
    //jdbc-url
    private String url;
    //jdbc驱动
    private String driver;
    //用户名
    private String username;
    //
    private String database;

    private String catalogName;

    private CatalogDialect<? extends Catalog> catalogDialect;

    public String getUrl() {
        return url;
    }

    public String getDriver() {
        //如果显式指定了驱动程序
        if (StringUtils.isNotEmpty(this.driver)) return driver;

        //获取catalog的默认驱动程序
        return this.catalogDialect.defaultDriverName();
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * 获取默认数据库
     * case 1. builder方法显式指定
     * <p>
     * new JdbcOptionBuilder()
     * .url("jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181")
     * .defaultDatabase("default")
     * .build()
     * </p>
     * case 2.从jdbc url中自动解析，成功与否取决于数据库方言的实现情况
     * new JdbcOptionBuilder()
     * .url("jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181/default")
     * .build()
     *
     * @return 默认数据库
     */
    public String getDatabase() {
        //如果用户显式指定了数据库名字
        if (this.database != null) return this.database;
        String catalogDialectDatabaseName = this.catalogDialect.getDatabaseName(this.url);
        if (catalogDialectDatabaseName != null) return catalogDialectDatabaseName;
        throw new UnsupportedOperationException("必须要在url中指定数据库名字或者在builder构造器中指定数据库名字");
    }


    public CatalogDialect<? extends Catalog> getCatalogDialect() {
        return catalogDialect;
    }

    /**
     * 获取catalog名称，如果获取不到就调用dialect获取默认值
     */
    public String getCatalogName() {
        if (this.catalogName == null) {
            return this.catalogDialect.defaultCatalogName();
        }
        return this.catalogName;
    }

    public JdbcOption(DescriptorProperties descriptorProperties) {
        super(descriptorProperties);

        //设置受支持的jdbcCatalogDialect
        descriptorProperties
                .getOptionalString(CATALOG_JDBC_BASE_URL)
                .flatMap(CatalogDialectServices::get)
                .ifPresent(this::setCatalogDialect);

        //设置驱动程序，假如没有设置，就从dialect里获取默认驱动程序
        this.driver = descriptorProperties.getOptionalString(CATALOG_JDBC_DRIVER)
                .orElse(this.catalogDialect.defaultDriverName());

        descriptorProperties.getOptionalString(CATALOG_JDBC_USERNAME).ifPresent(this::setUsername);
        descriptorProperties.getOptionalString(CATALOG_JDBC_BASE_URL).ifPresent(this::setUrl);

        this.catalogName = catalogName;
    }


    @Override
    public String toString() {
        return "JdbcOption{" +
                "url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", database='" + database + '\'' +
                ", catalogName='" + catalogName + '\'' +
                ", catalogDialect=" + catalogDialect +
                '}';
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public void setCatalogDialect(CatalogDialect<? extends Catalog> catalogDialect) {
        this.catalogDialect = catalogDialect;
    }

//    /**
//     * 构造一个jdbc配置类
//     * 泛型指定为{@link CatalogDialect}的实现类，如果该数据库支持catalog
//     * <code>
//     * new JdbcOptionBuilder()//泛型指定为phoenix的catalog实现类
//     * .url("jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181/default")//加载jdbcUrl，这是必须的
//     * .usernameAndPassword("root","123456")//数据库的用户名和密码，通常下都是成对出现，如果没有用户名和密码，请忽略使用此方法来设置用户名密码
//     * .build()
//     * </code>
//     */
//    public static class JdbcOptionBuilder {
//        private String url;
//        private String driver;
//        private String username;
//        private String password;
//        private String catalogName;
//        private String databaseName;
//
//        /**
//         * 设置jdbc-url
//         * example
//         * jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181
//         *
//         * @param url jdbc-url
//         * @return this
//         */
//        public JdbcOptionBuilder url(String url) {
//            this.url = url;
//            return this;
//        }
//
//        /**
//         * jdbc驱动程序，通常来说都内置了默认驱动程序，一般不需要主动 设置驱动程序
//         *
//         * @param driver jdbc驱动程序
//         * @return this
//         */
//        public JdbcOptionBuilder driver(String driver) {
//            Preconditions.checkArgument(
//                    StringUtils.isNotEmpty(driver),
//                    "驱动程序不能为空"
//            );
//            this.driver = driver;
//            return this;
//        }
//
//        /**
//         * 数据库用户名和密码
//         *
//         * @param username 用户名
//         * @param password 密码
//         * @return this
//         */
//        public JdbcOptionBuilder usernameAndPassword(String username, String password) {
//            Preconditions.checkArgument(
//                    StringUtils.isNotEmpty(username) &&
//                            StringUtils.isNotEmpty(password),
//                    "用户名和密码不能为空"
//            );
//            this.username = username;
//            this.password = password;
//            return this;
//        }
//
//        /**
//         * 用户名
//         */
//        public JdbcOptionBuilder username(String username) {
//            Preconditions.checkArgument(
//                    StringUtils.isNotEmpty(username),
//                    "用户名和密码不能为空"
//            );
//            this.username = username;
//            return this;
//        }
//
//        /**
//         * 密码
//         */
//        public JdbcOptionBuilder password(String password) {
//            Preconditions.checkArgument(
//                    StringUtils.isNotEmpty(password),
//                    "用户名和密码不能为空"
//            );
//            this.password = password;
//            return this;
//        }
//
//        /**
//         * catalog名字，默认为catalog实现类
//         *
//         * @param catalogName catalog名字
//         * @return this
//         */
//        public JdbcOptionBuilder catalogName(String catalogName) {
//            this.catalogName = catalogName;
//            return this;
//        }
//
//        /**
//         * 设置默认数据库名字
//         * 假如不在这个builder方法里显示指定，那么jdbc url上面也应该附带数据库名称
//         *
//         * @param defaultDatabaseName 默认数据库名字
//         * @return this
//         */
//        public JdbcOptionBuilder defaultDatabaseName(String defaultDatabaseName) {
//            Preconditions.checkArgument(
//                    StringUtils.isNotEmpty(defaultDatabaseName),
//                    "默认数据库不能为空"
//            );
//            this.databaseName = defaultDatabaseName;
//            return this;
//        }
//
//
//        /**
//         * 懂得都懂
//         *
//         * @return jdbc配置选项
//         */
//        public JdbcOption build() {
//            Preconditions.checkArgument(
//                    StringUtils.isNotEmpty(url),
//                    "jdbc-url不能为空"
//            );
//
//            //获取受支持的catalog
//            CatalogDialect<? extends Catalog> catalogDialect = CatalogDialectServices.getCatalogDialect(this.url);
//
//            return new JdbcOption(this.url, this.driver, this.username, this.password, this.catalogName, this.databaseName, catalogDialect);
//        }
    //   }
}
