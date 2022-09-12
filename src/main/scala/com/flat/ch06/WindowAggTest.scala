package com.flat.ch06

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowAggTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(e => e.timestamp)

    stream.keyBy(e => true)
      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5)))
      .aggregate( new PvUv )
      .print()

    env.execute()
  }

  class PvUv extends AggregateFunction[Event, (Long, Set[String]), Double] {
    override def createAccumulator(): (Long, Set[String]) = (0L, Set[String]())

    override def add(in: Event, acc: (Long, Set[String])): (Long, Set[String]) = {
      (acc._1 + 1, acc._2 + in.user)
    }

    override def getResult(acc: (Long, Set[String])): Double = acc._1.toDouble / acc._2.size

    override def merge(acc: (Long, Set[String]), acc1: (Long, Set[String])): (Long, Set[String]) = ???
  }
}
