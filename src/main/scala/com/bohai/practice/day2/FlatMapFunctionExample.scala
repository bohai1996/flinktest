package com.bohai.practice.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.fromElements("white","black","gray")
    stream.flatMap(new MyFlatMapFunction)
      .print()

    stream.flatMap(new FlatMapFunction[String,String] {
      override def flatMap(t: String, collector: Collector[String]): Unit = {
        if (t == "white"){
          collector.collect(t)
        }else if (t == "black"){
          collector.collect(t)
          collector.collect(t)
        }
      }
    })

    env.execute()
  }

  class MyFlatMapFunction extends FlatMapFunction[String,String]{
    override def flatMap(t: String, collector: Collector[String]): Unit = {
      if (t == "white"){
        collector.collect(t)
      }else if(t == "black"){
        collector.collect(t)
        collector.collect(t)
      }
    }
  }

}
