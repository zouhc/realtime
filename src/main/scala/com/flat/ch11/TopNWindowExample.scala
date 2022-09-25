package com.flat.ch11

import com.flat.ch05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object TopNWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val eventStream = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 5*1000L),
      Event("Cary", "./home", 60*1000L),
      Event("Bob", "./prod?id=3", 90*1000L),
      Event("Alice", "./prod?id=7", 100095*1000L),
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
      new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      }
    ))

    val eventTable: Table = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").as("ts"), $("et").rowtime())
    tableEnv.createTemporaryView("eventTable", eventTable)

    // 基于窗口聚合计算每个用户的访问量
    val countWindowTable = tableEnv.sqlQuery(
      """
        |select
        | user, count(url) as cnt, window_start, window_end
        | from table (
        | tumble(table eventTable, descriptor(et), interval '1' hour)
        | )
        | group by user, window_start, window_end
        |""".stripMargin)
    tableEnv.createTemporaryView("countWindowTable", countWindowTable)

    // 提取cnt值最大的两个用户
    val top2Result = tableEnv.sqlQuery(
      """
        |select
        | *
        |from (
        | select *, row_number() over (
        | partition by window_start, window_end
        | order by cnt desc
        | ) as row_num
        | from countWindowTable
        |)
        |where row_num <= 2
        |""".stripMargin)

    tableEnv.toDataStream(top2Result).print()

    env.execute()
  }
}
