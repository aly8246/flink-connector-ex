package com.github.aly8246.format.output;

import com.github.aly8246.option.JdbcContext;
import org.apache.flink.api.common.io.RichOutputFormat;

public abstract class AbstractJdbcOutputFormat<T> extends RichOutputFormat<T> {
    protected JdbcContext context;

}
