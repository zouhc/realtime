package com.flat.ch05

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


class MyFlatMap extends FlatMapFunction[Event, String] {
  override def flatMap(t: Event, collector: Collector[String]): Unit = {
     if (t.user == "Mary") {
       collector.collect(t.user)
     } else if (t.user == "Bob") {
       collector.collect(t.user)
       collector.collect(t.url)
     }
  }
}

object TransfromFlatMapTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./cart", 1000L)
    )

    stream.flatMap(new MyFlatMap).print("1")

    stream.flatMap(t => Array(t.user)).print("2")

    env.execute()
  }
}
