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
    private Integer fetchSize;

    /**
     * 从配置文件创建
     * 这里简单匹配一个密码，从配置文件中搜素带有password的配置来设置为密码
     * 假如你的配置里有多个包含password的选项，则需要自己来修正password
     */
    public BaseOption(DescriptorProperties descriptorProperties) {
        descriptorProperties.getOptionalBoolean(ASYNC_SUPPORT_KEY).ifPresent(this::setAsyncSupported);
        this.fetchSize = descriptorProperties.getOptionalInt(SOURCE_FETCH_SIZE).orElse(SOURCE_FETCH_SIZE_DEFAULT);
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

    public Integer getFetchSize() {
        return fetchSize;
    }

    protected void setAsyncSupported(boolean asyncSupported) {
        this.asyncSupported = asyncSupported;
    }

    public String getPassword() {
        return password;
    }

    protected void setPassword(String password) {
        this.password = password;
    }
}
