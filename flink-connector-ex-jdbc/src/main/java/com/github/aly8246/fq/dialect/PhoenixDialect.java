//package com.github.aly8246.fq.dialect;
//
//import java.util.Arrays;
//import java.util.Optional;
//import java.util.stream.Collectors;
//
//public class PhoenixDialect implements JdbcDialect {
//    @Override
//    public boolean canHandle(String url) {
//        return url.startsWith("jdbc:phoenix:");
//    }
//
//    @Override
//    public boolean onlySupportUpsert() {
//        return true;
//    }
//
//    @Override
//    public Optional<String> defaultDriverName() {
//        return Optional.of("org.apache.phoenix.jdbc.PhoenixDriver");
//    }
//
//    @Override
//    public String quoteIdentifier(String identifier) {
//        return identifier;
//    }
//
//    @Override
//    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
//        String columns = Arrays.stream(fieldNames)
//                .map(this::quoteIdentifier)
//                .collect(Collectors.joining(", "));
//        String placeholders = Arrays.stream(fieldNames)
//                .map(f -> "?")
//                .collect(Collectors.joining(", "));
//
//        //修改为phoenix独有的upsert
//        return Optional.of("UPSERT INTO " + quoteIdentifier(tableName) +
//                "(" + columns + ")" + " VALUES (" + placeholders + ")");
//    }
//}
