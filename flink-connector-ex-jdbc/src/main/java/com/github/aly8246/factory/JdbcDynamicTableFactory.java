package com.github.aly8246.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public class JdbcDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "jdbc-ex";


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        //TODO        validateConfigOptions(config);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        System.out.println("JdbcDynamicTableFactory.createDynamicTableSource");
        return null;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database url.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc table name.");

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
