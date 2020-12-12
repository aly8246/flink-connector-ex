package com.github.aly8246.factory;

import com.github.aly8246.descriptor.JdbcDescriptor;
import com.github.aly8246.option.JdbcOption;
import com.github.aly8246.option.JdbcSourceSinkContext;
import com.github.aly8246.source.JdbcTableSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * 支持flink-sql的jdbc读写操作
 */
public class JdbcExSourceSinkFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {
    private final JdbcDescriptor jdbcDescriptor = new JdbcDescriptor();

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> map) {
        return null;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
        //验证配置文件
        DescriptorProperties descriptorProperties = jdbcDescriptor.validatedProperties(map);

        //读取配置文件
        JdbcOption jdbcOption = new JdbcOption(descriptorProperties);

        //创建运行上下文对象
        JdbcSourceSinkContext context = new JdbcSourceSinkContext(jdbcOption, map);

        //创建tableSource
        return new JdbcTableSource(context);
    }

    /**
     * 通过某些字段来选择是否支持该ddl
     */
    @Override
    public Map<String, String> requiredContext() {
        return new JdbcDescriptor().sourceSinkFactoryRoutingSelector();
    }

    /**
     * flink-sql支持的配置
     */
    @Override
    public List<String> supportedProperties() {
        return new JdbcDescriptor().supportedProperties();
    }
}
