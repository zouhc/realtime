package com.flat.ch05

import org.apache.flink.streaming.api.scala._

object PartitionBroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val datastream = env.addSource(new ClickSource)

    datastream.broadcast.print("broadcast").setParallelism(4)

    env.execute()
  }
}
