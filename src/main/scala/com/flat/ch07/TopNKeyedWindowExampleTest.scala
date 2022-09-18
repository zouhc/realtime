package com.flat.ch07

import com.flat.ch05.{ClickSource, Event}
import com.flat.ch06.UrlViewCount
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable

object TopNKeyedWindowExampleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    //
    var result = stream.keyBy(data => data.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new UrlAggregate(), new UrlWindowProcess())

    result.keyBy(_.end)
      .process(new TopN(2))
      .print()

    env.execute()
  }

  class TopN(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
    var urlCountState: ListState[UrlViewCount] = _

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val urlViewCountList = urlCountState.get().toList
      val topList = urlViewCountList.sortBy(-_.count).take(n)

      //进行结果输出
      val result = new StringBuilder()
      result.append(s"=========窗口 ${timestamp-1-10000} ~ ${timestamp -1}\n")
      for (i <- topList.indices) {
        val viewCount = topList(i)
        result.append(s"TopN = $i\t url=${viewCount.url}\t 浏览量=${viewCount.count}\n")
      }

      out.collect(result.toString())
    }

    override def open(parameters: Configuration): Unit = {
      urlCountState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))
    }

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      urlCountState.add(i)

      context.timerService().registerEventTimeTimer(i.end + 1) // 加一毫秒（当流中关闭一个窗口，开启新窗口后，老窗口的数据都已计算完）
    }
  }

  class UrlAggregate() extends AggregateFunction[Event, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }

  class UrlWindowProcess() extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, elements.iterator.next(), context.window.getStart, context.window.getEnd))
    }
  }

}
