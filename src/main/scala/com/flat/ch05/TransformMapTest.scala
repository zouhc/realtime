package com.flat.ch05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

class UserExtractor extends MapFunction[Event, String] {
  override def map(t: Event): String = t.user
}

object TransformMapTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L)
    )

    stream.map(e => e.user).print("1")

    stream.map(new UserExtractor).print("2")

    env.execute()
  }
}
