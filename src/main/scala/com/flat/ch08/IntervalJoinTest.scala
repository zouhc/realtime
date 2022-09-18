package com.flat.ch08

import com.flat.ch05.Event
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 订单流
    val orderStream = env.fromElements(
      ("Mary", "order1", 1000L),
      ("Mary", "order2", 5000L),
      ("Mary", "order3", 8000L),
    ).assignAscendingTimestamps(_._3)

    val clickStream = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Mary", "./fav", 4000L),
      Event("Mary", "./detail", 6000L),
    ).assignAscendingTimestamps(_.timestamp)

    orderStream.keyBy(_._1).intervalJoin(clickStream.keyBy(_.user))
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new ProcessJoinFunction[(String, String, Long), Event, String] {
        override def processElement(left: (String, String, Long), right: Event, context: ProcessJoinFunction[(String, String, Long), Event, String]#Context, collector: Collector[String]): Unit = {
          collector.collect(s"$left -> $right")
        }
      })
      .print()

    env.execute()
  }

}
