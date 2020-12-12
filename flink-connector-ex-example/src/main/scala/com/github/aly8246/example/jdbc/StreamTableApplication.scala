package com.github.aly8246.example.jdbc

import com.github.aly8246.example.EnvCreate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 从数据库读取表数据
 */
object StreamTableApplication {
  def main(args: Array[String]): Unit = {
    val envTuple: (StreamExecutionEnvironment, StreamTableEnvironment) = EnvCreate.createEnv(args)

    //创建流表
    envTuple._2.sqlUpdate(
      """
        |create table stream_user(
        | id bigint comment '用户ID',
        | name varchar comment '用户名称',
        | sex varchar comment '用户性别'
        | )
        | with(
        | 'connector.type' = 'jdbc-ex',
        | 'jdbc.url' = 'jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181',
        | 'jdbc.driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',
        | 'jdbc.table' = 'dim_user'
        | )
        |""".stripMargin)

    //输出流表查询结果
    envTuple._2.sqlQuery("select * from stream_user where id= 1 or (sex = '男' and name like '小%') and 1=1")
      .toAppendStream[Row]
      .print("流表条件部分输出")

    envTuple._1.execute()
  }
}
