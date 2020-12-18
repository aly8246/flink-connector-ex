package com.github.aly8246.example.jdbc

import com.github.aly8246.catalog.JdbcOption.JdbcOptionBuilder
import com.github.aly8246.catalog.{JdbcOption, PhoenixJdbcCatalog}
import com.github.aly8246.example.EnvCreate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.catalog.Catalog

/**
 * 从数据库读取表数据
 */
object CatalogApplication {
  def main(args: Array[String]): Unit = {
    val envTuple: (StreamExecutionEnvironment, StreamTableEnvironment) = EnvCreate.createEnv(args)

    val jdbcOption: JdbcOption = new JdbcOptionBuilder().url("").build()

    //根据连接信息创建catalog
    val phoenixCatalog: Catalog = new PhoenixJdbcCatalog(jdbcOption)
    envTuple._2.registerCatalog("phoenix", phoenixCatalog)
    //    envTuple._2.useCatalog("phoenix")
    //catalog注册到tableEnv
    //查询catalog的schema
    envTuple._1.socketTextStream("www.baidu.com", 80).print()
    envTuple._2.fromValues("aaa").fetch(1).printSchema()
    //envTuple._2.sqlQuery("select * from phoenix.`DEFAULT`.dim_user")
    envTuple._2.sqlQuery("select * from phoenix.`default`.dim_user")

    envTuple._1.execute()
  }
}
