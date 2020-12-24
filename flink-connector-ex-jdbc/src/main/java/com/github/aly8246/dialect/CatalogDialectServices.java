package com.github.aly8246.dialect;

import org.apache.flink.table.catalog.Catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

public class CatalogDialectServices {
    public static List<CatalogDialect<? extends Catalog>> catalogDialectList = new ArrayList<>();

    static {
        //获取线程安全的类加载器
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        //通过spi机制加载未知的数据库方言
        ServiceLoader<CatalogDialect> catalogDialectServiceLoader = ServiceLoader.load(CatalogDialect.class, loader);
        for (CatalogDialect<? extends Catalog> jdbcDialect : catalogDialectServiceLoader) {
            catalogDialectList.add(jdbcDialect);
        }
    }

    public static CatalogDialect<? extends Catalog> getCatalogDialect(String jdbcUrl) {
        return catalogDialectList.stream()
                .filter(e -> e.supported(jdbcUrl))
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException("数据库可能不支持catalog"));
    }

    public static Optional<CatalogDialect<? extends Catalog>> get(String url) {
        return catalogDialectList
                .stream()
                .filter(e -> e.supported(url))
                .findFirst();
    }
}
