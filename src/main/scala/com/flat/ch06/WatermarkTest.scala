package com.flat.ch06

import com.flat.ch05.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration

object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500) // 周期生成水位线

    val stream: DataStream[Event] = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Mary", "./home", 4000L),
      Event("Bob", "./cart", 1000L),
      Event("Mary", "./home", 5000L),
      Event("Bob", "./cart", 3000L)
    )

    // 1. 有序流
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]().withTimestampAssigner(
      new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = {
          t.timestamp
        }
      }
    )).print()

    //2. 乱序流
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](
      Duration.ofSeconds(2) // 延迟2秒
    ).withTimestampAssigner(
      new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(t: Event, l: Long): Long = {
          t.timestamp
        }
      }
    )).print()

    // 3. 自定义生成水位线
    stream.assignTimestampsAndWatermarks(new WatermarkStrategy[Event]() {
      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Event] = {
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = {
            t.timestamp
          }
        }
      }

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Event] = {
        new WatermarkGenerator[Event] {
          //定义延迟时间
          val delay = 500L
          // 定义属性保存最大时间戳
          var maxTs = Long.MinValue + delay +1

          override def onEvent(t: Event, l: Long, watermarkOutput: WatermarkOutput): Unit = {
            maxTs = math.max(maxTs, t.timestamp)
          }

          override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
            val watermark = new Watermark(maxTs - delay -1)
            watermarkOutput.emitWatermark(watermark)
          }
        }
      }
    }
    ).print()

    env.execute()
  }
}
