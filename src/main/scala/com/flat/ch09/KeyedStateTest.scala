package com.flat.ch09

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }


  class MyFlatMap extends RichFlatMapFunction[Event, String] {
    var valueState: ValueState[Event] = _
    var listState: ListState[Event] = _
    var mapState: MapState[String, Long] = _
    var reduceState: ReducingState[Event] = _
    var aggregateState: AggregatingState[Event, String] = _

    override def open(parameters: Configuration): Unit = {
      valueState = getRuntimeContext.getState(new ValueStateDescriptor[Event]("my-value", classOf[Event]))
      listState = getRuntimeContext.getListState(new ListStateDescriptor[Event]("my-list", classOf[Event]))
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("my-map", classOf[String], classOf[Long]))

      reduceState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Event]("my-reduce",
        new ReduceFunction[Event] {
          override def reduce(t: Event, t1: Event): Event = {
            Event(t.user, t.url, t1.timestamp)
          }
        },
        classOf[Event]))

      aggregateState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, Long, String]("my-agg",
        new AggregateFunction[Event, Long, String] {
          override def createAccumulator(): Long = 0L

          override def add(in: Event, acc: Long): Long = acc + 1

          override def getResult(acc: Long): String = s"agg后的值：${acc}"

          override def merge(acc: Long, acc1: Long): Long = ???
        },
        classOf[Long]
      ))
    }

    override def close(): Unit = {
      valueState.clear()
    }

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      collector.collect(s"状态值： ${valueState.value()}")
      valueState.update(in)
      collector.collect(s"状态值： ${valueState.value()}")

      collector.collect(s"List 状态值：${listState.get()}")
      listState.add(in)

      val v = if (mapState.contains(in.user)) mapState.get(in.user) else 0
      collector.collect(s"Map 状态值： ${v}")
      mapState.put(in.user, v+1)

      collector.collect(s"Reduce 状态值： ${reduceState.get()}")
      reduceState.add(in)

      collector.collect(s"agg 状态值：${aggregateState.get()}")
      aggregateState.add(in)

      collector.collect("=========================")
    }
  }
}
