//package com.github.aly8246.fq.dialect;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Optional;
//import java.util.ServiceLoader;
//
//public class JdbcDialectService implements Serializable {
//    //已经注册的数据方言
//    public static final List<JdbcDialect> JDBC_DIALECT_LIST = new ArrayList<>();
//
//    static {
//        //获取线程安全的类加载器
//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
//
//        //通过spi机制加载未知的数据库方言
//        ServiceLoader<JdbcDialect> jdbcDialectServiceLoader = ServiceLoader.load(JdbcDialect.class, loader);
//        for (JdbcDialect jdbcDialect : jdbcDialectServiceLoader) {
//            JDBC_DIALECT_LIST.add(jdbcDialect);
//        }
//    }
//
//    /**
//     * 根据url选择数据库方言，由{@link JdbcDialect} 提供的canHandle方法来决定
//     *
//     * @param url jdbcUrl
//     * @return optional
//     */
//    public static Optional<JdbcDialect> get(String url) {
//        return JDBC_DIALECT_LIST
//                .stream()
//                .filter(e -> e.canHandle(url))
//                .findFirst();
//    }
//}
