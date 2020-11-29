package com.bohai.practice.day2

import com.bohai.dsapi.FlatMapExample.MyFlatMapFunction
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      (1,"aaaa") ,
      (2,"bbbb")
    )

    val stream2 = env.fromElements(
      (1,"cccc"),
      (2,"dddd")
    )

    val connected = stream1.keyBy(r=>r._1)
      .connect(stream2.keyBy(r=>r._1))

    val connected1 = stream1.connect(stream2).keyBy(0,0)

    connected1.flatMap(new MyFlatMapFunction)
      .print()

    env.execute()

  }

  class MyFlatMapFunction extends CoFlatMapFunction[(Int,String),(Int,String),String]{
    override def flatMap1(in1: (Int, String), collector: Collector[String]): Unit = {
      collector.collect(in1._2 + "来自第一条流的元素发送两次")
      collector.collect(in1._2 + "来自第一条流的元素发送两次")
    }

    override def flatMap2(in2: (Int, String), collector: Collector[String]): Unit = {
      collector.collect(in2._2 + "来自第二条流的元素发送一次")
    }
  }


}
