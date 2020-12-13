package com.github.aly8246.example.jdbc

import com.github.aly8246.example.EnvCreate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 流表同步join维表
 */
object KafkaTableASyncJoinDimTableApplication {
  def main(args: Array[String]): Unit = {
    val envTuple: (StreamExecutionEnvironment, StreamTableEnvironment) = EnvCreate.createEnv(args)

    //{"id":1 ,"user_id":1 ,"name":"小米手机坏了","price":100,"phone_type":"家用","buy_time":"2020-12-09 12:10:13"}
    //{"id":2 ,"user_id":1 ,"name":"华为手机","price":200,"phone_type":"办公","buy_time":"2020-12-08 11:52:11"}
    //{"id":3 ,"user_id":2 ,"name":"大米手机又坏了","price":200,"phone_type":"办公","buy_time":"2020-12-08 11:52:11"}

    //定义kafka流表
    envTuple._2.sqlUpdate(
      """
        |create table stream_phone(
        |   id bigint comment '主键id',
        |   user_id bigint comment '用户',
        |   name varchar comment '手机名称',
        |   price int comment '手机价格',
        |   phone_type varchar comment '手机类型',
        |   buy_time varchar comment '购买时间',
        |   process_time as PROCTIME()
        |)
        |with (
        |   'connector.type' = 'kafka',
        |   'connector.version' = 'universal',
        |   'connector.topic' = 'test_phone',
        |	  'connector.properties.group.id' = 'flink_consumer',
        |   'connector.startup-mode' = 'latest-offset',
        |   'connector.properties.zookeeper.connect' = 'hadoop1:2181,hadoop2:2181,hadoop3:2181',
        |   'connector.properties.bootstrap.servers' = 'hadoop1:9092,hadoop2:9092,hadoop3:9092',
        |   'format.type' = 'json'
        |)
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
        | sp.id as stream_pid,
        | sp.name as stream_phone_name,
        | sp.price as stream_phone_price,
        | sp.phone_type as stream_phone_type,
        | du.id as dim_user_id,
        | du.name as dim_username,
        | du.sex as dim_user_sex
        |from stream_phone sp
        |left join dim_user FOR SYSTEM_TIME AS OF sp.process_time as du
        |on sp.user_id = du.id
        |""".stripMargin)
      .toRetractStream[Row]
      .print("流表异步join维表")

    envTuple._1.execute()
  }

}
