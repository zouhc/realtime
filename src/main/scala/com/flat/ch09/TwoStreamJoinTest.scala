package com.flat.ch09

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.convert.ImplicitConversions._

object TwoStreamJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromElements(
      ("a", 1000L),
      ("b", 1000L),
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 4000L),
      ("b", 3000L),
    ).assignAscendingTimestamps(_._2)

    stream1.keyBy(_._1).connect(stream2.keyBy(_._1))
      .process(new MyJoinProcess)
      .print()

    env.execute()
  }

  class MyJoinProcess extends CoProcessFunction[(String, Long), (String, Long), String] {
    lazy val list1State = getRuntimeContext.getListState(new ListStateDescriptor[(String, Long)]("list-1", classOf[(String, Long)]))
    lazy val list2State = getRuntimeContext.getListState(new ListStateDescriptor[(String, Long)]("list-2", classOf[(String, Long)]))

    override def processElement1(in1: (String, Long), context: CoProcessFunction[(String, Long), (String, Long), String]#Context, collector: Collector[String]): Unit = {
      list1State.add(in1)

      for (v <- list2State.get()) {
        collector.collect(s"$in1 => $v")
      }
    }

    override def processElement2(in2: (String, Long), context: CoProcessFunction[(String, Long), (String, Long), String]#Context, collector: Collector[String]): Unit = {
      list2State.add(in2)

      for (v <- list1State.get()) {
        collector.collect(s"$v => $in2")
      }
    }
  }
}
