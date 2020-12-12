package com.github.aly8246.descriptor;

import com.github.aly8246.common.BaseDescriptor;
import com.github.aly8246.dialect.JdbcDialect;
import com.github.aly8246.dialect.JdbcDialectService;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JdbcDescriptor extends BaseDescriptor {
    //connector类型
    public static final String CONNECTOR_VALUE = "jdbc-ex";

    //jdbc连接信息
    public static final String CONNECTOR_URL = "jdbc.url";
    public static final String CONNECTOR_TABLE = "jdbc.table";
    public static final String CONNECTOR_DRIVER = "jdbc.driver";
    public static final String CONNECTOR_USERNAME = "jdbc.username";
    public static final String CONNECTOR_PASSWORD = "jdbc.password";

    //source参数
    public static final String SOURCE_FETCH_SIZE = "source.fetch-size";

    @Override
    public List<String> supportedProperties() {
        List<String> supportedProperties = super.supportedProperties();
        supportedProperties.add(CONNECTOR_VALUE);
        supportedProperties.add(CONNECTOR_URL);
        supportedProperties.add(CONNECTOR_TABLE);
        supportedProperties.add(CONNECTOR_DRIVER);
        supportedProperties.add(CONNECTOR_USERNAME);
        supportedProperties.add(CONNECTOR_PASSWORD);

        supportedProperties.add(SOURCE_FETCH_SIZE);
        return supportedProperties;
    }

    @Override
    public Map<String, String> sourceSinkFactoryRoutingSelector() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_VALUE);
        return context;
    }

    @Override
    public DescriptorProperties validatedProperties(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        //验证schema
        new SchemaValidator(true,
                false,
                false
        ).validate(descriptorProperties);

        //我也验证一下
        this.validate(descriptorProperties);
        return descriptorProperties;
    }

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        this.validateConnectionProperties(properties);
        this.validateCommonProperties(properties);
    }

    /**
     * 验证数据库连接信息配置是否正确
     */
    private void validateConnectionProperties(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_URL, false, 1);
        properties.validateString(CONNECTOR_TABLE, false, 1);
        properties.validateString(CONNECTOR_DRIVER, true);
        properties.validateString(CONNECTOR_USERNAME, true);
        properties.validateString(CONNECTOR_PASSWORD, true);

        //从配置文件中获取数据库连接url
        final String url = properties.getString(CONNECTOR_URL);
        //根据url选择方言
        Optional<JdbcDialect> dialect = JdbcDialectService.get(url);
        //检查是否有支持的数据库方言
        Preconditions.checkState(dialect.isPresent(), "没有dialect支持该jdbc-url:" + url);

        //是否包含密码
        Optional<String> password = properties.getOptionalString(CONNECTOR_PASSWORD);
        if (password.isPresent()) {
            //假如输入了数据库用户名，则必须要输入密码
            Preconditions.checkArgument(
                    properties.getOptionalString(CONNECTOR_USERNAME).isPresent(),
                    "Database username must be provided when database password is provided"
            );
        }
    }
}
