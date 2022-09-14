package com.flat.ch07

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object processFuncTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .process(new ProcessFunction[Event, String] {
        override def processElement(i: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
          if (i.user == "Mary") {
            collector.collect(i.user)
          } else if (i.user == "Bob") {
            collector.collect(i.user)
            collector.collect(i.url)
          }

          collector.collect(s"当前水位线：${context.timerService().currentWatermark()}")
          collector.collect(s"当前时间：${context.timestamp()}, Event数据的时间：${i.timestamp}")
          collector.collect(s"当前处理的子任务： ${getRuntimeContext.getIndexOfThisSubtask}")
          collector.collect(s"作业ID: ${getRuntimeContext.getJobId}")
        }
      }).print("test")

    env.execute()
  }
}
