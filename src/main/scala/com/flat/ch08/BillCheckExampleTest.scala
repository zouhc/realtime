package com.flat.ch08

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BillCheckExampleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("order1", "success", 1000L),
        ("order2", "success", 2000L)
    ).assignAscendingTimestamps(_._3)

    val stream2 = env.fromElements(
      ("order1", "success", "wechat", 4000L),
      ("order3", "success", "alipay", 7000L)
    ).assignAscendingTimestamps(_._4)

    val jionStream = stream1.connect(stream2)
    jionStream.keyBy(d1 => d1._1, d2 => d2._1)
      .process(new CoProcessFunction[(String, String, Long), (String, String, String, Long), String] {
        var appEvent: ValueState[(String, String, Long)] = _
        var thirdpartEvent: ValueState[(String, String, String, Long)] = _

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
          if (appEvent.value() != null) {
            out.collect(s"${appEvent.value()._1}, 对账失败，第三方支付平台事件未到")
          }

          if (thirdpartEvent.value() != null) {
            out.collect(s"${thirdpartEvent.value()._1}, 对账失败，app支付事件未到")
          }

          appEvent.clear()
          thirdpartEvent.clear()
        }

        override def open(parameters: Configuration): Unit = {
          appEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, Long)]("app-event", classOf[(String, String, Long)]))
          thirdpartEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, String, Long)]("thirdpath-event", classOf[(String, String, String, Long)]))
        }

        override def processElement1(value: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, collector: Collector[String]): Unit = {
          if (thirdpartEvent.value() != null) {
            collector.collect(s"对账成功, ${value._1}")
            thirdpartEvent.clear()
          } else {
            context.timerService().registerEventTimeTimer(value._3 + 5000)
            appEvent.update(value)
          }
        }

        override def processElement2(value: (String, String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, collector: Collector[String]): Unit = {
          if (appEvent.value() != null) {
            collector.collect(s"对账成功， ${value._1}")
            appEvent.clear()
          } else {
            context.timerService().registerEventTimeTimer(value._4 + 5000)
            thirdpartEvent.update(value)
          }
        }
      }).print()

    env.execute()

  }
}
