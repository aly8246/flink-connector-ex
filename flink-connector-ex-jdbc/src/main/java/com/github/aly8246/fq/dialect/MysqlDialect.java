//package com.github.aly8246.fq.dialect;
//
//import java.util.Arrays;
//import java.util.Optional;
//import java.util.stream.Collectors;
//
//public class MysqlDialect implements JdbcDialect {
//    @Override
//    public boolean canHandle(String url) {
//        return url.startsWith("jdbc:mysql:");
//    }
//
//    @Override
//    public boolean onlySupportUpsert() {
//        return false;
//    }
//
//    @Override
//    public Optional<String> defaultDriverName() {
//        return Optional.of("com.mysql.jdbc.Driver");
//    }
//
//    @Override
//    public String quoteIdentifier(String identifier) {
//        return "`" + identifier + "`";
//    }
//
//    /**
//     * mysql也支持upsert 不过是通过指定ON DUPLICATE KEY UPDATE来完成upsert功能
//     *
//     * @param tableName       表名
//     * @param fieldNames      字段名
//     * @param uniqueKeyFields 主键
//     */
//    @Override
//    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
//        String updateClause = Arrays.stream(fieldNames)
//                .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
//                .collect(Collectors.joining(", "));
//        return Optional.of(getInsertIntoStatement(tableName, fieldNames) +
//                " ON DUPLICATE KEY UPDATE " + updateClause
//        );
//    }
//}
