package com.flat.ch09

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FakeWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .process(new MyProcess(10000))
      .print()

    env.execute()
  }

  class MyProcess(size: Int) extends KeyedProcessFunction[String, Event, String] {
    // 使用一个mapstate来分窗口存储 
    lazy val windowPvState = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("w-state", classOf[Long], classOf[Long]))

    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
      // 通过事件时间划分窗口
      val start = i.timestamp / size *size
      val end = start + size

      // 注册定时器
      context.timerService().registerEventTimeTimer(end - 1)

      // 更窗口计数
      if (windowPvState.contains(start)) {
        val pv= windowPvState.get(start)
        windowPvState.put(start, pv+1)
      } else {
        windowPvState.put(start, 1)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 窗口触发，结果输出
      val start = timestamp+1 - size
      val end = start + size
      out.collect(s"url=${ctx.getCurrentKey}, pv=${windowPvState.get(start)}, 窗口：$start ~ $end")

      // 窗口计算结束后移除状态
      windowPvState.remove(start)
    }
  }
}
