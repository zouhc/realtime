package com.flat.ch05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object SourceKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()

    props.setProperty("bootstrap.servers", "localhost")
    props.setProperty("group.id", "consumer_group")
    props.setProperty("key.descrializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.descrializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), props)
    )

    stream.print("kafka")

    env.execute()


  }
}
