package com.flat.ch06

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FullWindowAggTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    stream.assignAscendingTimestamps(e => e.timestamp)
      .keyBy(e => "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UV)
      .print()

    env.execute()
  }

  // 全窗口函数
  class UV extends ProcessWindowFunction[Event, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      var uvs = Set[String]()

      elements.foreach(e => uvs += e.user)
      val uv = uvs.size

      out.collect(s"窗口：${context.window.getStart} ~${context.window.getEnd}的UV: $uv")
    }
  }
}
