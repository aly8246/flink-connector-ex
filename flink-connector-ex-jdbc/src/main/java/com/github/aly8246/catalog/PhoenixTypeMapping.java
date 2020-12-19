package com.github.aly8246.catalog;

import com.github.aly8246.client.JdbcConnector;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.Map;

public class PhoenixTypeMapping {
    public static DataType fromJdbcTypeId(Integer type, Integer size) {
        switch (type) {
            case -6:
                return DataTypes.TINYINT();
            case -5:
                return DataTypes.BIGINT();
            case -3:
                return DataTypes.VARBINARY(size);
            case -2:
                return DataTypes.BINARY(size);
            case 1:
                return DataTypes.CHAR(size);
            case 3:
                return DataTypes.DECIMAL(size, 18);
            case 4:
                return DataTypes.INT();
            case 5:
                return DataTypes.SMALLINT();
            case 6:
                return DataTypes.FLOAT();
            case 8:
                return DataTypes.DOUBLE();
            case 12:
                return DataTypes.VARCHAR(size);
            case 16:
                return DataTypes.BOOLEAN();
            case 91:
                return DataTypes.DATE();
            case 92:
                return DataTypes.TIME();
            case 93:
                return DataTypes.TIMESTAMP();
            default:
                throw new UnsupportedOperationException("暂未支持的数据类型ID:" + type);
        }
    }

}
