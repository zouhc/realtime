package com.flat.ch08

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang

object CogroupJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("a", 1000L),
      ("a", 1000L),
      ("a", 8000L),
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 1000L),
      ("a", 1000L),
      ("a", 1000L),
    ).assignAscendingTimestamps(_._2)

    // 可以做左外连接，右外连接，内连接
    stream1.coGroup(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new CoGroupFunction[(String, Long), (String, Long), String] {
        override def coGroup(iterable: lang.Iterable[(String, Long)], iterable1: lang.Iterable[(String, Long)], collector: Collector[String]): Unit = {
          collector.collect(s"$iterable -> $iterable1")
        }
      }).print()

    env.execute()
  }
}
