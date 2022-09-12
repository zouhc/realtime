package com.flat.ch05

import org.apache.flink.streaming.api.scala._

object SourceCustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new ClickSource).setParallelism(10)

    dataStream.print()

    env.execute()
  }
}
