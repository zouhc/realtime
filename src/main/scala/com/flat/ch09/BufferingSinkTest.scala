package com.flat.ch09

import com.flat.ch05.{ClickSource, Event}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import scala.collection.convert.ImplicitConversions._

import scala.collection.mutable.ListBuffer

object BufferingSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .addSink( new BufferingSink(5))

    env.execute()
  }

  class BufferingSink(size: Int) extends SinkFunction[Event] with CheckpointedFunction {
    // 定义列表状态，保存要缓冲的数据
    var bufferedState: ListState[Event] = _
    // 定义本地变量列表
    val bufferedList = ListBuffer[Event]()

    override def invoke(value: Event, context: SinkFunction.Context): Unit = {
      bufferedList += value

      if (bufferedList.size == size) {
        bufferedList.foreach(e => println(e))
        bufferedList.clear()
        println("=============================== 输出完毕 =========================")
      }
    }

    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
      bufferedState.addAll(bufferedList)
    }

    override def initializeState(ctx: FunctionInitializationContext): Unit = {
      bufferedState = ctx.getOperatorStateStore.getListState(new ListStateDescriptor[Event]("buffered-list", classOf[Event]))

      // 如果从故障中恢复，那么就将状态中的数据添加到局部变量中
      if (ctx.isRestored) {
        for (data <- bufferedState.get()) {
          bufferedList += data
        }
      }
    }
  }
}
