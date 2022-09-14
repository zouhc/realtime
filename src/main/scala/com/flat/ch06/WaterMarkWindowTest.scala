package com.flat.ch06

import com.flat.ch05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration

object WaterMarkWindowTest {
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
      .keyBy(e => e.user)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .trigger(new CleanTrigger) //此处只是测试，TumblingEventTimeWindows已使用了默认的EventTimeTrigger
      .evictor(new MyEvictor)
      .aggregate(new UserPvUvAgg, new UserPVUV)
      .print()

    env.execute()

  }

  class MyEvictor extends Evictor[Event, TimeWindow] {
    override def evictBefore(iterable: lang.Iterable[TimestampedValue[Event]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      val it = iterable.iterator()
      while (it.hasNext) {
        if (it.next().getValue.user.toLowerCase == "test") {
          it.remove()
        }
      }
    }

    override def evictAfter(iterable: lang.Iterable[TimestampedValue[Event]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {

    }
  }

  class CleanTrigger extends Trigger[Event, TimeWindow] {
    override def onElement(t: Event, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
      println("测试： 窗口关闭时，释放状态资源")
    }
  }

  class UserPvUvAgg extends AggregateFunction[Event, (Long, Set[String]), (Long, Set[String])] {
    override def createAccumulator(): (Long, Set[String]) = (0L, Set[String]())

    override def add(in: Event, acc: (Long, Set[String])): (Long, Set[String]) = (acc._1 + 1, acc._2 + in.user)

    override def getResult(acc: (Long, Set[String])): (Long, Set[String]) = acc

    override def merge(acc: (Long, Set[String]), acc1: (Long, Set[String])): (Long, Set[String]) = {
      (acc._1+acc1._1, acc._2++acc1._2)
    }
  }

  class UserPVUV extends ProcessWindowFunction[(Long, Set[String]), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(Long, Set[String])], out: Collector[String]): Unit = {
      val value = elements.iterator.next()
      val pv = value._1
      val uv = value._2

      out.collect(s"user ${key}的 PV: ${pv}, UV: ${uv}")
    }
  }
}
