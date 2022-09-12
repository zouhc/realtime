package com.flat.ch05

import org.apache.flink.streaming.api.scala._

object PartitionRescaleTest {
def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream = env.addSource(new ClickSource)

    stream.rescale.print("rescale").setParallelism(4)

    env.execute()
  }
}
