package com.flat.ch05

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object PartitionCustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    stream.partitionCustom(new Partitioner[Event] {
      override def partition(k: Event, i: Int): Int = {
        if (k.url == "./home") {
          1
        } else {
          2
        }
      }
    }, data => data).print().setParallelism(4)

    env.execute()
  }
}
