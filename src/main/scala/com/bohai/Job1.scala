package com.bohai


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time



object Job1 {
  def main(args: Array[String]): Unit = {
//    printf("234234")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    
    val text: DataStream[String] = env.socketTextStream("localhost",9999,'\n')
    
    val wc = text
      .flatMap{w => w.split("\\s")}
      .map{w => WordWithCount(w,1)}
      .keyBy("w")
      .timeWindow(Time.seconds(5))
      .sum("i")

    wc.print().setParallelism(1)
    env.execute("socket window wc")
  }

  case class WordWithCount(w: String, i: Int)

}
