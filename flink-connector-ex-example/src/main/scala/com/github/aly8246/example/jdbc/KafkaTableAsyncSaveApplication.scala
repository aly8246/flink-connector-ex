package com.github.aly8246.example.jdbc

import com.github.aly8246.example.EnvCreate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 流表同步join维表
 */
object KafkaTableAsyncSaveApplication {
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


    //创建输出表
    envTuple._2.sqlUpdate(
      """
     create table save_phone(
        |   id bigint comment '主键id',
        |   user_id bigint comment '用户',
        |   name varchar comment '手机名称',
        |   price int comment '手机价格',
        |   phone_type varchar comment '手机类型',
        |   buy_time varchar comment '购买时间'
        |)
        | with(
        | 'connector.type' = 'jdbc-ex',
        | 'async-support' = 'true',
        | 'jdbc.url' = 'jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181',
        | 'jdbc.driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',
        | 'jdbc.table' = 'save_phone'
        | )
        |""".stripMargin)

    //流表落盘
    envTuple._2.sqlUpdate(
      """
        |upsert into save_phone
        |select id,user_id,name,price,phone_type,buy_time from stream_phone
        |""".stripMargin)

    envTuple._1.execute()
  }

}
