package com.flat.ch02

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 读取文件本文件数据
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host", "localhost")
    val port = params.getInt("port", 9999)
    val lineDataStream = env.socketTextStream(host, port)

    // 3.对数据进行转换
    val wordAndOne = lineDataStream.flatMap( _.split("\\s+") ).map(word => (word, 1))

    // 4. 按单词进行分组
    val wordAndOneGroup = wordAndOne.keyBy( data => data._1)

    // 5. 对分组数据里进行sum聚合统计
    val sum = wordAndOneGroup.sum(1)

    // 6. 打印输出
    sum.print()

    // 7. 执行任务
    env.execute()
  }

}
