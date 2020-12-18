//package com.github.aly8246.fq.dialect;
//
//import org.apache.flink.util.Preconditions;
//
//import java.io.Serializable;
//import java.util.Arrays;
//import java.util.Optional;
//import java.util.stream.Collectors;
//
//public interface JdbcDialect extends Serializable {
//    /**
//     * 检查是否有可用的数据库方言来支持该url
//     *
//     * @param url jdbc url
//     */
//    boolean canHandle(String url);
//
//    /**
//     * 是否只支持upsert，不支持insert和update
//     * 比如phoenix数据库
//     */
//    boolean onlySupportUpsert();
//
//    /**
//     * 默认驱动程序
//     */
//     default  Optional<String> defaultDriverName() {
//        return Optional.empty();
//    }
//
//    /**
//     * 假如输入一个数据库保留字段，则需要转移
//     * select `count` from mysql;
//     * select 'count' from phoenix;
//     *
//     * @param identifier 数据库保留字段
//     * @return 转义后的字段
//     */
//    default String quoteIdentifier(String identifier) {
//        return "\"" + identifier + "\"";
//    }
//
//    /**
//     * 根据表和字段名称，来拼接一个upsert语句
//     *
//     * @param tableName       表名
//     * @param fieldNames      字段名
//     * @param uniqueKeyFields 主键
//     */
//    default Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
//        Preconditions.checkArgument(onlySupportUpsert(), "假如数据库仅支持upsert,则必须override getUpsertStatement");
//        return Optional.empty();
//    }
//
//    /**
//     * 查询某些数据是否存在
//     *
//     * @param tableName       表名
//     * @param conditionFields 条件字段
//     */
//    default String getRowExistsStatement(String tableName, String[] conditionFields) {
//        String fieldExpressions = Arrays.stream(conditionFields)
//                .map(f -> quoteIdentifier(f) + "=?")
//                .collect(Collectors.joining(" AND "));
//        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
//    }
//
//    /**
//     * 生成insert语句
//     *
//     * @param tableName  表名
//     * @param fieldNames 字段名
//     */
//    default String getInsertIntoStatement(String tableName, String[] fieldNames) {
//        String columns = Arrays.stream(fieldNames)
//                .map(this::quoteIdentifier)
//                .collect(Collectors.joining(", "));
//        String placeholders = Arrays.stream(fieldNames)
//                .map(f -> "?")
//                .collect(Collectors.joining(", "));
//        return "INSERT INTO " + quoteIdentifier(tableName) +
//                "(" + columns + ")" + " VALUES (" + placeholders + ")";
//    }
//
//    /**
//     * 生成更新语句
//     *
//     * @param tableName       表名
//     * @param fieldNames      字段名
//     * @param conditionFields 条件字段
//     */
//    default String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
//        String setClause = Arrays.stream(fieldNames)
//                .map(f -> quoteIdentifier(f) + "=?")
//                .collect(Collectors.joining(", "));
//        String conditionClause = Arrays.stream(conditionFields)
//                .map(f -> quoteIdentifier(f) + "=?")
//                .collect(Collectors.joining(" AND "));
//        return "UPDATE " + quoteIdentifier(tableName) +
//                " SET " + setClause +
//                " WHERE " + conditionClause;
//    }
//
//    /**
//     * 生成删除语句
//     *
//     * @param tableName       表名
//     * @param conditionFields 删除条件
//     */
//    default String getDeleteStatement(String tableName, String[] conditionFields) {
//        String conditionClause = Arrays.stream(conditionFields)
//                .map(f -> quoteIdentifier(f) + "=?")
//                .collect(Collectors.joining(" AND "));
//        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
//    }
//
//    /**
//     * 生成查询语句
//     *
//     * @param tableName       表名
//     * @param selectFields    字段名
//     * @param conditionFields 查询条件
//     */
//    default String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
//        String selectExpressions = Arrays.stream(selectFields)
//                .map(this::quoteIdentifier)
//                .collect(Collectors.joining(", "));
//        String fieldExpressions = Arrays.stream(conditionFields)
//                .map(f -> quoteIdentifier(f) + "=?")
//                .collect(Collectors.joining(" AND "));
//        return "SELECT " + selectExpressions + " FROM " +
//                quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
//    }
//}
