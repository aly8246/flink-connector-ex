package com.github.aly8246.utils;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;

import org.apache.flink.table.types.logical.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.SimpleFormatter;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

public class JdbcUtil implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(JdbcUtil.class);

    /**
     * 为stmt填充值
     */
    public static void setFields(PreparedStatement statement, String[] lookupKeys, Object[] lookupValues, TableSchema schema) {
        for (int i = 0; i < lookupKeys.length; i++) {
            int stmtIndex = i + 1;

            DataType dataType = schema.getFieldDataType(lookupKeys[i]).get();

            LogicalType dataTypeLogicalType = dataType.getLogicalType();
            try {
                if (dataTypeLogicalType instanceof BigIntType) {
                    statement.setLong(stmtIndex, Long.parseLong(lookupValues[i].toString()));
                } else if (dataTypeLogicalType instanceof TinyIntType) {
                    statement.setInt(stmtIndex, Integer.parseInt(lookupKeys[i]));
                } else if (dataTypeLogicalType instanceof IntType) {
                    statement.setInt(stmtIndex, Integer.parseInt(lookupKeys[i]));
                } else if (dataTypeLogicalType instanceof BooleanType) {
                    statement.setBoolean(stmtIndex, Boolean.parseBoolean(lookupValues[i].toString()));
                } else if (dataTypeLogicalType instanceof DecimalType) {
                    statement.setBigDecimal(stmtIndex, new BigDecimal(lookupKeys[i]));
                } else if (dataTypeLogicalType instanceof DoubleType) {
                    statement.setDouble(stmtIndex, Double.parseDouble(lookupKeys[i]));
                } else if (dataTypeLogicalType instanceof FloatType) {
                    statement.setFloat(stmtIndex, Float.parseFloat(lookupKeys[i]));
                } else if (dataTypeLogicalType instanceof VarCharType) {
                    statement.setString(stmtIndex, lookupKeys[i]);
                } else if (dataTypeLogicalType instanceof TimestampType) {
                    statement.setTimestamp(stmtIndex, new Timestamp(Long.parseLong(lookupValues[i].toString())));
                } else if (dataTypeLogicalType instanceof DateType) {
                    java.util.Date sourceDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(lookupKeys[i]);
                    statement.setDate(stmtIndex, new Date(sourceDate.getTime()));
                } else if (dataTypeLogicalType instanceof TimeType) {
                    java.util.Date sourceDate = new SimpleDateFormat("hh:mm:ss").parse(lookupKeys[i]);
                    statement.setTime(stmtIndex, new Time(sourceDate.getTime()));
                }
            } catch (SQLException | ParseException ex) {
                ex.printStackTrace();
            }
        }
    }

    static {
        HashMap<TypeInformation<?>, Integer> m = new HashMap<>();
        m.put(BYTE_TYPE_INFO, Types.TINYINT);
        m.put(SHORT_TYPE_INFO, Types.SMALLINT);
        m.put(INT_TYPE_INFO, Types.INTEGER);
        m.put(LONG_TYPE_INFO, Types.BIGINT);
        m.put(FLOAT_TYPE_INFO, Types.REAL);
        m.put(DOUBLE_TYPE_INFO, Types.DOUBLE);
        m.put(SqlTimeTypeInfo.DATE, Types.DATE);
        m.put(SqlTimeTypeInfo.TIME, Types.TIME);
        m.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
        m.put(BIG_DEC_TYPE_INFO, Types.DECIMAL);
        m.put(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);

        HashMap<Integer, String> names = new HashMap<>();
        names.put(Types.VARCHAR, "VARCHAR");
        names.put(Types.SMALLINT, "SMALLINT");
        names.put(Types.INTEGER, "INTEGER");
        names.put(Types.BIGINT, "BIGINT");
        names.put(Types.FLOAT, "FLOAT");
        names.put(Types.DOUBLE, "DOUBLE");
        names.put(Types.CHAR, "CHAR");
        names.put(Types.DATE, "DATE");
        names.put(Types.TIME, "TIME");
    }

}
