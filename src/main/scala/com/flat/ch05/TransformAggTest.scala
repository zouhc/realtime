package com.flat.ch05

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

class MyKeySelector extends KeySelector[Event, String] {
  override def getKey(in: Event): String = in.user
}

object TransformAggTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./cart", 1000L),
      Event("Mary", "./prod?id=1", 6000L)
    )

    stream.keyBy(t => t.user).maxBy("timestamp")
      .print()

//    stream.keyBy(new MyKeySelector)
    env.execute()
  }
}
