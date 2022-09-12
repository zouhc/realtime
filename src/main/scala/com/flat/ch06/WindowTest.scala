package com.flat.ch06

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]()
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
      }))
      .map(e => (e.user, 1))
      .keyBy(e => e._1)
//      .window(TumblingProcessingTimeWindows.of(Time.hours(1), Time.minutes(-1)))
//      .window(TumblingEventTimeWindows.of(Time.hours(1)))
//      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
//      .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce((state, data) => (state._1, state._2 + data._2))
      .print()

    env.execute()
  }

}
