package com.github.aly8246.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object EnvCreate {
  def createEnv(args: Array[String]): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    val bst: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bst)
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    (env, tEnv)
  }

}
