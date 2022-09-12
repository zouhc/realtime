package com.flat.ch05

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkToRedisTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    // 创建配置
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(3306)
      .build()

    stream.addSink(new RedisSink[Event](conf, new MyRedisMapper))

    env.execute()
  }

  class MyRedisMapper extends RedisMapper[Event] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "clicks")
    }

    override def getKeyFromData(t: Event): String = t.user

    override def getValueFromData(t: Event): String = t.url
  }


}
