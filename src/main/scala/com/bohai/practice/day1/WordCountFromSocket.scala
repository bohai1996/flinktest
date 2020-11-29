package com.bohai.practice

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WordCountFromSocket {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

//    val stream = env
//      // 数据来源自`nc -lk 9999`，先启动nc
//      .socketTextStream("localhost", 9999, '\n')
//      // 使用空格分割字符串 `\\s`表示空格
//      .flatMap(w => w.split("\\s"))
//      // 相当于MapReduce中的Map操作
//      .map(w => (w, 1))
//      // shuffle操作
//      .keyBy(_._1)
//      // 聚合`count`字段
//      .sum(1)
//
//    stream.print()

    env.execute()
  }

}
