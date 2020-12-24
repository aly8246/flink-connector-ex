package com.github.aly8246.context;

import com.github.aly8246.common.AbstractConnectorContext;
import com.github.aly8246.option.JdbcOption;

import java.util.Map;

public class JdbcConnectorContextImpl extends AbstractConnectorContext<JdbcOption> implements JdbcConnectorContext {

    public JdbcConnectorContextImpl(JdbcOption option, Map<String, String> properties) {
        super(option, properties);
    }
}
