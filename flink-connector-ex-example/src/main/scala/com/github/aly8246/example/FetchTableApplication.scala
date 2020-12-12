package com.github.aly8246.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 从数据库读取表数据
 */
object FetchTableApplication {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val bst: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bst)
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    tEnv.sqlUpdate(
      """
        |create table dim_user(
        | id bigint comment '用户ID',
        | name varchar comment '用户名称',
        | sex varchar comment '用户性别'
        | )
        | with(
        | 'connector.type' = 'jdbc-ex',
        | 'async-support' = 'true',
        | 'jdbc.url' = 'jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181',
        | 'jdbc.driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',
        | 'jdbc.table' = 'dim_user',
        | 'jdbc.username' = '',
        | 'jdbc.password' = ''
        | )
        |""".stripMargin)

    tEnv.sqlQuery("select * from dim_user where id= 1 or (sex = '男' and name like '小%') and 1=1")
      .toAppendStream[Row]
      .print("条件部分输出")


    //    tEnv.sqlQuery(
    //      """
    //        |select * from dim_user
    //        |""".stripMargin)
    //      .toAppendStream[Row]
    //      .print("全部输出")

    env.execute()
  }
}
