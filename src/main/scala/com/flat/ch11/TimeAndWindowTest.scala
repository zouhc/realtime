package com.flat.ch11

import com.flat.ch05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala._

import java.time.Duration

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 1. 在创建表时指定时间属性
    tableEnv.executeSql(
      """
        |create table eventTable (
        |uid string,
        |url string,
        |ts bigint,
        |et as to_timestamp(from_unixtime(ts/1000)),
        |watermark for et as et - interval '5' second
        |) with (
        |'connector' = 'filesystem',
        |'path' = 'input/clicks.txt',
        |'format' = 'csv'
        |)
        |""".stripMargin)

    tableEnv.from("eventTable").printSchema()

    // 2. 将流转换成表时指定时间属性
    val eventStream = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 5*1000L),
      Event("Cary", "./home", 60*1000L),
      Event("Bob", "./prod?id=3", 90*1000L),
      Event("Alice", "./prod?id=7", 105*1000L),
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
    .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
      override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
    }))

    val eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp") as("ts"),
      $("et").rowtime())

//    val eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").rowtime() as("ts"))

    // 测试累计窗口
    //为方便SQL引用，在环境在注册表eventTable
    tableEnv.createTemporaryView("eventTable", eventTable)
    tableEnv.sqlQuery(
      """
        |select
        | user, window_end, count(url) as cnt
        | from table (
        | cumulate(
        |   table eventTable,
        |   descriptor(et),
        |   interval 10 minute,
        |   interval 1 hour
        | )
        | )
        | group by user, window_start, window_end
        |""".stripMargin)

    tableEnv.toDataStream(eventTable).print()

//    eventTable.printSchema()

    // 测试开窗聚合
    val overResultTable = tableEnv.sqlQuery(
      """
        |select
        | user, url, ts, avg(ts) over w as avg_ts
        |from eventTable
        | window w as (
        |   partition by user
        |   order by et
        |   rows between 3 preceding and current row
        |)
        |""".stripMargin)

    tableEnv.toDataStream(overResultTable).print("over")


    env.execute()
  }
}
