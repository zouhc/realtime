package com.flat.ch08

import org.apache.flink.streaming.api.scala._

object ConnectTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromElements(1, 2, 4)
    val stream2 = env.fromElements(1.2, 2.3, 4.5, 5, 9)

    stream1.connect(stream2)
      .map(
        v1 => s"流1： $v1",
        v2 => s"流2：$v2"
      )
      .print()

    env.execute()
  }
}
