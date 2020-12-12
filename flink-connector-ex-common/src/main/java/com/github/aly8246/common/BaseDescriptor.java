package com.github.aly8246.common;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.Schema.*;

public abstract class BaseDescriptor extends ConnectorDescriptorValidator {
    private static final Logger log = LoggerFactory.getLogger(BaseDescriptor.class);

    //开启异步表支持
    public static final String ASYNC_SUPPORT_KEY = "async-support";
    //默认为同步
    public static final Boolean ASYNC_SUPPORT_VALUE_DEFAULT = false;


    //累计缓存多少行后写入db
    public static final String WRITE_FLUSH_MAX_ROWS = "sink.buffer-flush.max-rows";
    //累计多少秒后写入db
    public static final String WRITE_FLUSH_INTERVAL = "sink.buffer-flush.interval";

    //source参数
    //读取出错后的重试次数
    public static final String SOURCE_MAX_RETRIES = "source.max-retries";
    public static final Long SOURCE_MAX_RETRIES_DEFAULT = 3L;
    //最多缓存多少行数据
    public static final String SOURCE_CACHE_ROWS = "source.cache.rows";
    public static final Long SOURCE_CACHE_ROWS_DEFAULT = 10000L;
    //缓存最长时间
    public static final String SOURCE_CACHE_TTL = "source.cache.ttl";
    public static final Long SOURCE_CACHE_TTL_DEFAULT = 1000L * 30;
    //做为stream加载表的时候一次fetch多少条数据
    public static final String SOURCE_FETCH_SIZE = "source.fetch-size";
    public static final Long SOURCE_FETCH_SIZE_DEFAULT = 10000L;

    //写入累计重试次数
    public static final String WRITE_MAX_RETRIES = "sink.max-retries";


    //schema
    public static final String CONNECTOR_SCHEMA_DATA_TYPE = SCHEMA + ".#." + SCHEMA_DATA_TYPE;
    public static final String CONNECTOR_SCHEMA_NAME = SCHEMA + ".#." + SCHEMA_NAME;
    public static final String CONNECTOR_SCHEMA_SCHEMA_EXPR = SCHEMA + ".#." + TABLE_SCHEMA_EXPR;

    //watermark
    public static final String CONNECTOR_SCHEMA_WATERMARK_ROW_TIME = SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME;
    public static final String CONNECTOR_SCHEMA_WATERMARK_STRATEGY_EXPR = SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR;
    public static final String CONNECTOR_SCHEMA_WATERMARK_STRATEGY_DATA_TYPE = SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE;

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
    }

    /**
     * 用哪个配置选项来支持你的sourceSinkFactory
     */
    abstract public Map<String, String> sourceSinkFactoryRoutingSelector();

    /**
     * 获取验证后的配置
     */
    abstract public DescriptorProperties validatedProperties(Map<String, String> properties);

    /**
     * 支持的配置参数
     */
    public List<String> supportedProperties() {
        List<String> supportedList = new ArrayList<>();
        //异步支持
        supportedList.add(ASYNC_SUPPORT_KEY);

        //sink buffer支持
        supportedList.add(WRITE_FLUSH_MAX_ROWS);
        supportedList.add(WRITE_FLUSH_INTERVAL);
        supportedList.add(WRITE_MAX_RETRIES);

        //source参数支持
        supportedList.add(SOURCE_FETCH_SIZE);
        supportedList.add(SOURCE_MAX_RETRIES);
        supportedList.add(SOURCE_CACHE_ROWS);
        supportedList.add(SOURCE_CACHE_TTL);

        //表字段支持
        supportedList.add(CONNECTOR_SCHEMA_DATA_TYPE);
        supportedList.add(CONNECTOR_SCHEMA_NAME);
        supportedList.add(CONNECTOR_SCHEMA_SCHEMA_EXPR);

        //watermark支持
        supportedList.add(CONNECTOR_SCHEMA_WATERMARK_ROW_TIME);
        supportedList.add(CONNECTOR_SCHEMA_WATERMARK_STRATEGY_EXPR);
        supportedList.add(CONNECTOR_SCHEMA_WATERMARK_STRATEGY_DATA_TYPE);


        return supportedList;
    }

    /**
     * 一些基础的配置边界验证
     */
    protected void validateCommonProperties(DescriptorProperties properties) {
        properties.validateString(ASYNC_SUPPORT_KEY, true);

        //验证异步表支持是否超出边界
        properties.getOptionalString(ASYNC_SUPPORT_KEY).ifPresent(e ->
                Preconditions.checkArgument(
                        e.toLowerCase().equals("true") || e.toLowerCase().equals("false"),
                        "年轻人,请选择是否要为表开启异步支持，不要乱来，不要来骗，来偷袭69岁码保国，要讲码德，要耗子尾汁"
                )
        );

        //验证写入row number边界
        properties.getOptionalString(WRITE_FLUSH_MAX_ROWS).ifPresent(e -> {
            long maxRows = Long.parseLong(e);
            if (maxRows > 200000) {
                log.warn(maxRows + "行数据后再统一写入，可能会造成写入延迟太高");
            }
            Preconditions.checkArgument(maxRows == 0, "一次最少写入一条数据");
        });

        //验证写入row 时间边界
        properties.getOptionalString(WRITE_FLUSH_INTERVAL).ifPresent(e -> {
            long maxInterval = Long.parseLong(e);
            if (maxInterval > 120) {
                log.warn(maxInterval + "秒数据后再统一写入，可能会造成写入延迟太高");
            }
        });

        //重试次数验证
        properties.getOptionalString(WRITE_MAX_RETRIES).ifPresent(e -> {
            long maxRetries = Long.parseLong(e);
            if (maxRetries > 20) {
                log.warn(maxRetries + "20次重试次数可能会导致数据库并发异常");
            }
            Preconditions.checkArgument(maxRetries == 0, "sink.max-retries必须大于1");
        });
    }

    /**
     * 连表配置选项验证
     */
    protected void validateLookupProperties(DescriptorProperties properties) {
        properties.validateLong(SOURCE_FETCH_SIZE, true);
        properties.validateLong(SOURCE_MAX_RETRIES, true);
        properties.validateDuration(SOURCE_CACHE_TTL, true, 1);
        properties.validateInt(SOURCE_CACHE_ROWS, true);
    }
}
