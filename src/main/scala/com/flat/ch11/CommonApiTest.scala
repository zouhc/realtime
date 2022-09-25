package com.flat.ch11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala._

object CommonApiTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建表环境
    // 1.1 直接基于流执行环境创建
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 1.2 传入一个环境配置参数创建
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnvironment = TableEnvironment.create(settings)

    // 2. 创建表
    tableEnv.executeSql(
      """
       create table eventTable (
       uid string,
       url string,
       ts bigint
       ) with (
       'connector' = 'filesystem',
       'path' = 'input/clicks.txt',
       'format' = 'csv'
       )
        """.stripMargin)

    // 3. 表的查询转换
    val resultTable = tableEnv.sqlQuery(
      """
        |select * from eventTable where uid='Bob'
        |""".stripMargin)
    resultTable.print()

    // 创建临时视图
    tableEnv.createTemporaryView("tmpResult", resultTable)

    tableEnv.sqlQuery(
      """
        |select * from tmpResult
        |""".stripMargin)
      .print()

    // 统计每个用户的分组
    val updateTable = tableEnv.sqlQuery(
      """
        |select uid, count(url) as pv from eventTable group by uid
        |""".stripMargin)



    // 4.输出表的创建
    tableEnv.executeSql(
      """
        |create table outputTable (
        |uid string,
        |path string,
        |ts bigint
        |) with (
        |'connector'='filesystem',
        |'path'='output',
        |'format'='csv'
        |)
        |""".stripMargin)


    resultTable.executeInsert("outputTable")

    // table转换成流，需要执行
    // 只append的表
    tableEnv.toDataStream(resultTable).print("result")
    // 对于有更新与删除的表
    tableEnv.toChangelogStream(updateTable).print("count")

    // 转换成流后需要流环境进行执行
    env.execute()

  }

}
