package com.flat.ch09

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object PeriodPvTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .process(new MyKedProcess())
      .print()

    env.execute()
  }

  class MyKedProcess extends KeyedProcessFunction[String, Event, String] {
    lazy val pvState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
    lazy val timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
      pvState.update(pvState.value()+1)

      if (timeState.value() == 0L) {
        context.timerService().registerEventTimeTimer(i.timestamp + 10000)
        //更新时间状态
        timeState.update(i.timestamp+10000)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"用户${ctx.getCurrentKey}的Pv=${pvState.value()}")
      timeState.clear()
    }
  }
}
