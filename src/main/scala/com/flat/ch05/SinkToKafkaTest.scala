package com.flat.ch05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object SinkToKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new ClickSource)

    dataStream.map(data => data.toString).addSink(
      new FlinkKafkaProducer[String]("localhost:9092", "clicks", new SimpleStringSchema())
    )

    env.execute()
  }
}
