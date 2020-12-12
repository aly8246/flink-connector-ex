package com.github.aly8246.common;

import org.apache.flink.table.descriptors.DescriptorProperties;

import java.io.Serializable;
import java.util.Map;

import static com.github.aly8246.common.BaseDescriptor.*;

public class BaseOption implements Serializable {
    //开启异步表支持
    protected boolean asyncSupported = ASYNC_SUPPORT_VALUE_DEFAULT;

    //密码
    protected String password;

    //一次加载多少数据
    private final Long fetchSize;
    //最多缓存多少行数据
    private final Long cacheRows;
    //最多缓存多少秒数据
    private final Long cacheTtl;

    /**
     * 从配置文件创建
     * 这里简单匹配一个密码，从配置文件中搜素带有password的配置来设置为密码
     * 假如你的配置里有多个包含password的选项，则需要自己来修正password
     */
    public BaseOption(DescriptorProperties descriptorProperties) {
        descriptorProperties.getOptionalBoolean(ASYNC_SUPPORT_KEY).ifPresent(this::setAsyncSupported);
        this.fetchSize = descriptorProperties.getOptionalLong(SOURCE_FETCH_SIZE).orElse(SOURCE_FETCH_SIZE_DEFAULT);
        this.cacheRows = descriptorProperties.getOptionalLong(SOURCE_CACHE_ROWS).orElse(SOURCE_CACHE_ROWS_DEFAULT);
        this.cacheTtl = descriptorProperties.getOptionalLong(SOURCE_CACHE_TTL).orElse(SOURCE_CACHE_TTL_DEFAULT);
        //简单匹配一个密码
        descriptorProperties
                .asMap()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().matches(".*password$"))
                .map(Map.Entry::getValue)
                .findFirst()
                .ifPresent(this::setPassword);
    }

    public Long getFetchSize() {
        return fetchSize;
    }

    public Long getCacheRows() {
        return cacheRows;
    }

    public Long getCacheTtl() {
        return cacheTtl;
    }

    public String getPassword() {
        return password;
    }


    protected void setAsyncSupported(boolean asyncSupported) {
        this.asyncSupported = asyncSupported;
    }


    protected void setPassword(String password) {
        this.password = password;
    }
}
