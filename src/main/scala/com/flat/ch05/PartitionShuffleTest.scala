package com.flat.ch05

import org.apache.flink.streaming.api.scala._

object PartitionShuffleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .shuffle.print("shuffle").setParallelism(4)

    env.execute()
  }

}
