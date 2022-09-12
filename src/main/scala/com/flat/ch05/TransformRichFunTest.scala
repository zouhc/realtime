package com.flat.ch05

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformRichFunTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Bob", "./prod?id=2", 1000L),
      Event("Alice", "./prod?id=1", 1000L),
      Event("Alice", "./cart", 1000L)
    )

    // 测试富函数功能
    stream.map(new RichMapFunction[Event, Long] {

      override def open(parameters: Configuration): Unit = {
        println("索引号为：" + getRuntimeContext.getIndexOfThisSubtask + "的任务")
      }

      override def close(): Unit = {
        println("当前任务结束")
      }

      override def map(in: Event): Long = {
        in.timestamp
      }
    }).print()

    env.execute()
  }
}
