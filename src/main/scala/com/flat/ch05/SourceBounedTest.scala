package com.flat.ch05

import org.apache.flink.streaming.api.scala._

case class Event(user: String, url: String, timestamp: Long)

object SourceBounedTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 从元素中读取数据
    val stream: DataStream[Int] = env.fromElements(1,3,4,5,6,7,8,89)
    val stream1: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L)
    )

    // 2. 从集合中读取数据
    val clicks = List(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L)
    )
    val stream2: DataStream[Event] = env.fromCollection(clicks)

   // 3. 从文件中读取数据
   val stream3: DataStream[String] = env.readTextFile("input/clicks.txt")


    stream.print("number")
    stream1.print("1")
    stream2.print("2")
    stream3.print("3")

    env.execute("test")
  }
}
