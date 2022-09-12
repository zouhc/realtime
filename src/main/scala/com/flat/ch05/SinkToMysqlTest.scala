package com.flat.ch05

import org.apache.flink.connector.jdbc._
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

object SinkToMysqlTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
//    val stream: DataStream[Event] = env.fromElements(
//      Event("Mary", "./home", 1000L),
//      Event("Bob", "./cart", 1000L)
//    )

    val sql = "insert into clicks (user, url) values (?,?)"

    stream.addSink( JdbcSink.sink(
      sql,
      new JdbcStatementBuilder[Event]() {
        override def accept(t: PreparedStatement, u: Event): Unit = {
          t.setString(1, u.user)
          t.setString(2, u.url)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(3000)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://localhost:3306/test")
        .withDriverName("com.mysql.cj.jdbc.Driver")
        .withUsername("root")
        .withPassword("qwertyuio")
        .build()
    )).setParallelism(2)

    env.execute()
  }
}
