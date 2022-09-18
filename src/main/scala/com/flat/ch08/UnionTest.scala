package com.flat.ch08

import com.flat.ch05.Event
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object UnionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.socketTextStream("localhost", 7777)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)
    val stream2 = env.socketTextStream("localhost", 8888)
      .map(data => {
        val fields = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)

    stream1.union(stream2)
      .process(new ProcessFunction[Event, String] {
        override def processElement(i: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
          collector.collect(s"当前水平线：${context.timerService().currentWatermark()}")
        }
      })
      .print()

    env.execute()


  }

}
