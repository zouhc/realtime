package com.flat.ch05

import org.apache.flink.streaming.api.scala._

object TransformReduceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Bob", "./prod?id=2", 1000L),
      Event("Bob", "./prod?id=1", 1000L),
      Event("Alice", "./cart", 1000L)
    )

    // 提取当前最活跃用户
    stream.map(user => (user, 1)).keyBy(user => user._1.user)
      .reduce((u1, u2) => {
        (u1._1, u1._2 + u2._2)
      })
//      .keyBy(u => u._1.user)
      .keyBy(data => true) //将所有数据按照同样的key分到同一个组中
      .reduce((state, data) => {
        if (data._2 > state._2) data else state
      })
      .print()

    env.execute()
  }

}
