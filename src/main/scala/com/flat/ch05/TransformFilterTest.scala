package com.flat.ch05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

class UserFilter extends FilterFunction[Event] {
  override def filter(t: Event): Boolean = t.user == "Bob"
}

object TransformFilterTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L)
    )

    stream.filter(e => e.url == "./home").print("1")

    stream.filter( new UserFilter).print("2")

    env.execute()
  }
}
