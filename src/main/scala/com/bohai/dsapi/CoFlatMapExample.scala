package com.bohai.dsapi

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1: DataStream[(String, Int)] = env.fromElements(
      ("aaaa",130),
      ("bbbb",100)
    )

    val stream2: DataStream[(String, Int)] = env.fromElements(
      ("aaaa",35),
      ("bbbb",33)
    )


    val connected: ConnectedStreams[(String, Int), (String, Int)] = stream1
      .keyBy(_._1).connect(stream2.keyBy(_._1))

    val printed: DataStream[String] = connected.flatMap(new MyCoFlatMapFunction)
    printed.print()
    env.execute()
  }

  class MyCoFlatMapFunction extends CoFlatMapFunction[(String,Int),(String,Int),String]{
    override def flatMap1(in1: (String, Int), out: Collector[String]): Unit = {
      out.collect(in1._1 + "的体重是："+in1._2+"斤")
      out.collect(in1._1 + "的体重是："+in1._2+"斤")
    }

    override def flatMap2(in2: (String, Int), out: Collector[String]): Unit = {
      out.collect(in2._1 + "的年龄是："+in2._2 + "岁")
    }
  }

}
