package com.flat.ch07

import com.flat.ch05.Event
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object EventTimeTimerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new CoustomSource)
      .assignAscendingTimestamps(data => data.timestamp)


    stream.keyBy(_.user)
      .process(new KeyedProcessFunction[String, Event, String] {
        override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
          val currentTime = context.timerService().currentWatermark()
          collector.collect(s"当前数据事件时间：${i.timestamp}, 水位线：${currentTime}")

          context.timerService().registerEventTimeTimer(currentTime + 5*1000)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect(s"触发定时器时间： $timestamp, 水位线：${ctx.timerService().currentWatermark()}")
        }
      })
      .print()

    env.execute()

  }


  class CoustomSource() extends RichParallelSourceFunction[Event] {
    override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
      sourceContext.collect(Event("Marhy", "./home", 1000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Marhy", "./home", 2000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Marhy", "./home", 1000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Marhy", "./home", 6000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Marhy", "./home", 4000L))
      Thread.sleep(5000L)
      sourceContext.collect(Event("Marhy", "./home", 7000L))
      Thread.sleep(5000L)
    }

    override def cancel(): Unit = ???
  }
}
