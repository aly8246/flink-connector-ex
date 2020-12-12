package com.github.aly8246.common;

import org.apache.flink.table.descriptors.DescriptorProperties;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import static com.github.aly8246.common.BaseDescriptor.*;

public class BaseOption implements Serializable {
    //开启异步表支持
    protected boolean asyncSupported = ASYNC_SUPPORT_VALUE_DEFAULT;

    //开启动态表支持
    protected boolean dynamicSupported = DYNAMIC_SUPPORT_VALUE_DEFAULT;

    //密码
    protected String password;

    /**
     * 从配置文件创建
     * 这里简单匹配一个密码，从配置文件中搜素带有password的配置来设置为密码
     * 假如你的配置里有多个包含password的选项，则需要自己来修正password
     */
    public BaseOption(DescriptorProperties descriptorProperties) {
        descriptorProperties.getOptionalBoolean(ASYNC_SUPPORT_KEY).ifPresent(this::setAsyncSupported);
        descriptorProperties.getOptionalBoolean(DYNAMIC_SUPPORT_KEY).ifPresent(this::setDynamicSupported);
        descriptorProperties.getOptionalBoolean(DYNAMIC_SUPPORT_KEY).ifPresent(this::setDynamicSupported);

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

    public boolean isAsyncSupported() {
        return asyncSupported;
    }

    public void setAsyncSupported(boolean asyncSupported) {
        this.asyncSupported = asyncSupported;
    }

    public boolean isDynamicSupported() {
        return dynamicSupported;
    }

    public void setDynamicSupported(boolean dynamicSupported) {
        this.dynamicSupported = dynamicSupported;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
