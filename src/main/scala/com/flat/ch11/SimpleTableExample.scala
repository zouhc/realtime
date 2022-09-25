package com.flat.ch11

import com.flat.ch05.Event
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

object SimpleTableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据源，创建DataStream
    val eventStream = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 5*1000L),
      Event("Cary", "./home", 60*1000L),
      Event("Bob", "./prod?id=3", 90*1000L),
      Event("Alice", "./prod?id=7", 105*1000L),
    )

    // 创建表环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 将DataStream转换成表
    val eventTable = tableEnv.fromDataStream(eventStream)

    //调用table Api进行转换操作
    val resultTable = eventTable.select($("url"), $("user"))
      .where($("user").isEqual("Alice"))

    // 直接写SQL
    val resultSqlTable = tableEnv.sqlQuery(s"select url, user from ${eventTable} where user='Bob'")

    // 转换成流打印输出
    tableEnv.toDataStream(resultTable).print("1")
    tableEnv.toDataStream(resultSqlTable).print("2")

    env.execute()
  }
}
