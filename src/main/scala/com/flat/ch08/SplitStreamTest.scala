package com.flat.ch08

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SplitStreamTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val maryStream = OutputTag[(String, String, Long)]("mary-tag")
    val bobStream = OutputTag[(String, String, Long)]("bob-tag")

    val stream = env.addSource(new ClickSource)
      .process(new ProcessFunction[Event, Event] {
        override def processElement(i: Event, context: ProcessFunction[Event, Event]#Context, collector: Collector[Event]): Unit = {
          i.user match {
            case "Mary" => context.output(maryStream, (i.user, i.url, i.timestamp))
            case "Bob" => context.output(bobStream, (i.user, i.url, i.timestamp))
            case _ => collector.collect(i)
          }
        }
      })

    stream.print("else")
    stream.getSideOutput(maryStream).print("mary")
    stream.getSideOutput(bobStream).print("bob")

    env.execute()
  }

}
