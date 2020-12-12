package com.github.aly8246.option;

import com.github.aly8246.common.BaseConnectorContext;

import java.util.Map;

public class JdbcSourceSinkContext extends BaseConnectorContext<JdbcOption> {
    public JdbcSourceSinkContext(JdbcOption option, Map<String, String> properties) {
        super(option, properties);
    }
}
