package com.flat.ch05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object TransformUDFTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Bob", "./prod?id=2", 1000L),
      Event("Alice", "./prod?id=1", 1000L),
      Event("Alice", "./cart", 1000L)
    )

    // test UDF函数
    stream.filter(new FilterFunction[Event] {
      override def filter(t: Event): Boolean = t.url.contains("prod")
    }).print()

    env.execute()
  }
}
