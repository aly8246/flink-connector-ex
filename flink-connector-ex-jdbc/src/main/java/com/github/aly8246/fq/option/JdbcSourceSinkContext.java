//package com.github.aly8246.fq.option;
//
//import com.github.aly8246.common.BaseConnectorContext;
//import com.github.aly8246.fq.dialect.JdbcDialect;
//
//import java.util.Map;
//
//public class JdbcSourceSinkContext extends BaseConnectorContext<JdbcOption> implements JdbcContext {
//    public JdbcSourceSinkContext(JdbcOption option, Map<String, String> properties) {
//        super(option, properties);
//    }
//
//    /**
//     * 获取一个最简单的。不带条件的select语句
//     * select * from dim_user
//     *
//     * @param selectFields 要查询的字段
//     * @return stmt
//     */
//    @Override
//    public String getQueryStmt(String[] selectFields) {
//        return this.getQueryStmt(selectFields, new String[0]);
//    }
//
//    /**
//     * 获取带查询条件的预编译语句
//     *
//     * @param selectFields      要查询的字段
//     * @param conditionKeyNames 要过滤的字段
//     * @return stmt
//     */
//    @Override
//    public String getQueryStmt(String[] selectFields, String[] conditionKeyNames) {
//        //获取表名称
//        String tableName = this.getOption().getTable();
//
//        //获取数据库方言
//        JdbcDialect jdbcDialect = this.getOption().getJdbcDialect();
//
//        //创建select的sql语句
//        return jdbcDialect.getSelectFromStatement(tableName, selectFields, conditionKeyNames);
//    }
//
////    /**
////     * 打开数据库连接
////     */
////    public Connection openConnection() {
////        Connection connection = null;
////        JdbcOption option = this.getOption();
////        try {
////            //和数据库建立连接
////            Class.forName(option.getJdbcDriver());
////            if (option.getUsername() != null) {
////                connection = DriverManager.getConnection(option.getUrl());
////            } else {
////                connection = DriverManager.getConnection(option.getUrl(), option.getUsername(), option.getPassword());
////            }
////
////            //自动提交
////            connection.setAutoCommit(true);
////        } catch (ClassNotFoundException | SQLException e) {
////            e.printStackTrace();
////        }
////
////        return connection;
////    }
//}
