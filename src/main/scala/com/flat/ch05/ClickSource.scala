package com.flat.ch05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

class ClickSource extends RichParallelSourceFunction[Event]{
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
    val random = new Random()

    // 定义数据生成逻辑
    val users = Array("Mary", "Alice", "Bob", "Cary")
    val urls = Array("./home", "./cart", "./fav", "./prod?=1", "./prod?=2", "./prod?=3")

    while (running) {
      val event = Event(users(random.nextInt(users.length)),
        urls(random.nextInt(urls.length)),
        Calendar.getInstance().getTimeInMillis)

      // 分配时间戳
      sourceContext.collectWithTimestamp(event, event.timestamp)

      // 发送数据
//      sourceContext.collect(event)

      //控流
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = running = false
}
