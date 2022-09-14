package com.flat.ch06

import com.flat.ch05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

object ProcessLateDataTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("localhost", 9999)
      .map(data => {
        try{
          val fields = data.split(",")
          if (fields.size >= 3) {
            Some(Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong))
          } else {
            None
          }
        } catch {
          case e: Exception => None
        }

      }).filter(e => e != None)
      .map(e => e.get)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](
        Duration.ofSeconds(2)
      ).withTimestampAssigner(
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      ))

    val outputTag = OutputTag[Event]("late-data")
    val result = stream.keyBy(e => e.url)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(outputTag)
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)

    result.print("result")
    stream.print("input")

    result.getSideOutput(outputTag).print("late data")

    env.execute()
  }
  class UrlViewCountAgg extends AggregateFunction[Event, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }

  class UrlViewCountResult extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(url: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(url, elements.iterator.next(), context.window.getStart, context.window.getEnd))
    }
  }


}
