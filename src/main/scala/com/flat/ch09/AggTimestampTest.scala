package com.flat.ch09

import apple.laf.JRSUIState.ValueState
import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object AggTimestampTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new MyFlatMap(5))
      .print()

    env.execute()
  }

  class MyFlatMap(size: Int) extends RichFlatMapFunction[Event, String] {
    lazy val aggTsState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, (Long, Long), Long]("agg-ts",
      new AggregateFunction[Event, (Long, Long), Long] {
        override def createAccumulator(): (Long, Long) = (0L, 0L)

        override def add(in: Event, acc: (Long, Long)): (Long, Long) = (acc._1+in.timestamp, acc._2+1)

        override def getResult(acc: (Long, Long)): Long = acc._1 / acc._2

        override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = ???
      },
      classOf[(Long, Long)]
    ))
    lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      val count = countState.value()
      countState.update(count+1)
      aggTsState.add(in)

      if (countState.value() == size) {
        collector.collect(s"用户${in.user} 的平均时间：${aggTsState.get()}")
        countState.clear()
      }
    }
  }
}
