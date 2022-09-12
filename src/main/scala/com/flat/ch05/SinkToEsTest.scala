package com.flat.ch05

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object SinkToEsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new ClickSource)

    // 定义ES主机列表
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 定义一个ES Sink function
    val esFun = new ElasticsearchSinkFunction[Event] {
      override def process(t: Event, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val data = new util.HashMap[String, String]
        data.put(t.user, t.url)

        // 包装要发送的http请求
        val indexRequest = Requests.indexRequest()
          .index("clicks")
          .source(data)

        // 发送请求
        requestIndexer.add(indexRequest)
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[Event](httpHosts, esFun).build())

    env.execute()
  }

}
