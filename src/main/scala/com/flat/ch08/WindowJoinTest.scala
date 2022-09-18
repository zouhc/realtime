package com.flat.ch08

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("a", 1000L),
        ("a", 1000L),
        ("a", 1000L),
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 1000L),
      ("a", 1000L),
      ("a", 1000L),
    ).assignAscendingTimestamps(_._2)

    stream1.join(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new JoinFunction[(String, Long), (String, Long), String] {
        override def join(in1: (String, Long), in2: (String, Long)): String = {
          s"${in1}  ->  ${in2}"
        }
      })
      .print()

    env.execute()
  }
}
