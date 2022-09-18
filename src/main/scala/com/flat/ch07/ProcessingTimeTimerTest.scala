package com.flat.ch07

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessingTimeTimerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.keyBy(data => true)
      .process(new KeyedProcessFunction[Boolean, Event, String] {
        override def processElement(i: Event, context: KeyedProcessFunction[Boolean, Event, String]#Context, collector: Collector[String]): Unit = {
          val currentTime = context.timerService().currentProcessingTime()
          collector.collect(s"数据到达，当前时间是：${currentTime}")

          // 注册一个5秒之后的定时器
          context.timerService().registerProcessingTimeTimer(currentTime + 5 * 1000)
        }

        // 定义定时器触发时执行的逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect(s"定时器触发，触发时间为：${timestamp}")
        }
      })
      .print()

    env.execute()
  }

}
