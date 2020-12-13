package com.github.aly8246.option;

import com.github.aly8246.common.SourceSinkContext;


public interface JdbcContext extends SourceSinkContext<JdbcOption> {

    /**
     * 根据要查询的字段生成sql
     *
     * @param selectFields 要查询的字段
     */
    public String getQueryStmt(String[] selectFields);

    //根据要查询的条件和and语句
    public String getQueryStmt(String[] selectFields, String[] conditionKeyNames);

}
