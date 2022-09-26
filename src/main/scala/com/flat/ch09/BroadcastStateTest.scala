package com.flat.ch09

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class Action(userName: String, action: String, role: String)
case class Pattern(action1: String, action2: String, isActive: Boolean, role: String)


object BroadcastStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 定义数据流，读取用户行为事件
    val actionStream = env.fromElements(
      Action("Bob", "login", "admin"),
      Action("Bob", "buy", "admin"),
      Action("Alice", "login", "admin"),
      Action("Alice", "pay", "admin"),
      Action("Bob", "login", "normal"),
      Action("Bob", "addcart", "normal"),
    )

    // 定义规则流，读取指定的行为模式
    val patternStream = env.fromElements(
      Pattern("login", "pay", true, "admin"),
      Pattern("login", "buy", true, "admin"),
      Pattern("login", "addcart", false, "normal"),
    )

    // 定义广播状态描述符
    val patterns = new MapStateDescriptor[Unit, List[Pattern]]("patterns", classOf[Unit], classOf[List[Pattern]])
    val broadcastStream = patternStream.broadcast(patterns)

    // 连接两种流
    actionStream.keyBy(data => data.userName + "_" + data.role)
      .connect(broadcastStream)
      .process(new PatternEvaluation)
      .print()

    env.execute()
  }

  class PatternEvaluation extends KeyedBroadcastProcessFunction[String, Action, Pattern, String] {
    // 定义值状态，保存上一次用户行为
    lazy val preActionState = getRuntimeContext.getState(new ValueStateDescriptor[Action]("preaction", classOf[Action]))

    override def processElement(in1: Action, readOnlyContext: KeyedBroadcastProcessFunction[String, Action, Pattern, String]#ReadOnlyContext, collector: Collector[String]): Unit = {
      //从广播状态中获取行为模板
      val patterns = readOnlyContext.getBroadcastState(new MapStateDescriptor[Unit, List[Pattern]]("patterns", classOf[Unit], classOf[List[Pattern]]))
        .get(Unit)
      //从值状态中读取上次行为
      val preAction = preActionState.value()

      if (patterns != null) {
        for (pattern <- patterns) {
          if (pattern != null && preAction != null) {
            if (pattern.action1 == preAction.action && pattern.action2 == in1.action && pattern.role == in1.role) {
              collector.collect(s" KEY：${readOnlyContext.getCurrentKey}, 用户名：${in1.userName}, pattern: ${pattern}")
            }
          }
        }
      }

      // 保存状态
      preActionState.update(in1)

    }

    override def processBroadcastElement(in2: Pattern, context: KeyedBroadcastProcessFunction[String, Action, Pattern, String]#Context, collector: Collector[String]): Unit = {
      val patternState = context.getBroadcastState(new MapStateDescriptor[Unit, List[Pattern]]("patterns", classOf[Unit], classOf[List[Pattern]]))

     val patterns = patternState.get(Unit)
      var newPatterns =  List[Pattern]()
      if (in2.isActive) {
        newPatterns = newPatterns.::(in2)
      }
      if (patterns != null) {

        newPatterns ++= patterns
      }

      patternState.put(Unit, newPatterns)

    }
  }
}
