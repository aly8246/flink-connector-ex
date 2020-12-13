package com.github.aly8246.example.jdbc

import com.github.aly8246.example.EnvCreate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 流表同步join维表
 */
object StreamTableASyncJoinDimTableApplication {
  def main(args: Array[String]): Unit = {
    val envTuple: (StreamExecutionEnvironment, StreamTableEnvironment) = EnvCreate.createEnv(args)

    //创建流表
    envTuple._2.sqlUpdate(
      """
        |create table stream_item(
        | id bigint comment '物品ID',
        | user_id bigint comment '用户id',
        | item_name varchar comment '物品名称',
        | process_time as PROCTIME()
        | )
        | with(
        | 'connector.type' = 'jdbc-ex',
        | 'jdbc.url' = 'jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181',
        | 'jdbc.driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',
        | 'jdbc.table' = 'dim_item'
        | )
        |""".stripMargin)

    //创建维表
    envTuple._2.sqlUpdate(
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
        | 'jdbc.table' = 'dim_user'
        | )
        |""".stripMargin)

    envTuple._2.sqlQuery(
      """
        |select
        | si.id as stream_iid,
        | si.item_name as stream_item_name,
        | du.id as dim_user_id,
        | du.name as dim_username,
        | du.sex as dim_user_sex
        |from stream_item si
        |left join dim_user FOR SYSTEM_TIME AS OF si.process_time as du
        |on si.user_id = du.id
        |""".stripMargin)
      .toRetractStream[Row]
      .print("流表异步join维表")

    envTuple._1.execute()
  }

}
