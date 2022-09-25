package com.flat.ch11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object TopNExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 创建表
    tableEnv.executeSql(
      """
        |create table eventTable (
        |uid string,
        |url string,
        |ts bigint,
        |et as to_timestamp(from_unixtime(ts/1000)),
        |watermark for et as et - interval '2' second
        |) with (
        | 'connector' = 'filesystem',
        | 'path' = 'input/clicks.txt',
        | 'format' = 'csv'
        |)
        |""".stripMargin)

    // topN 选择最活跃的前两个
    val countUrlTable = tableEnv.sqlQuery(
      """
        |select
        | uid, count(url) as cnt
        |from eventTable
        |group by uid
        |""".stripMargin)
    // 注册虚拟表
    tableEnv.createTemporaryView("countUrlTable", countUrlTable)
    // 提取count值最大的前两个用户
    val top2Result = tableEnv.sqlQuery(
      """
        |select
        | uid, cnt row_num
        |from (
        | select *, row_number() over (
        |   order by cnt desc
        | ) as row_num
        | from countUrlTable
        |)
        |where row_num <= 2
        |""".stripMargin)

    tableEnv.toChangelogStream(top2Result).print("topN")

    env.execute()
  }
}
