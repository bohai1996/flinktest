package com.bohai.dsapi

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("white","gray","black")

    val fm2: DataStream[String] = stream.flatMap(t=>t.split(""))
    fm2.print()

    val fm3: DataStream[Array[String]] = stream.map(t=>t.split(""))
    fm3.print()

    // `flatMap` 针对流中的每一个元素，生成0个，1个，或者多个数据
    val fm: DataStream[String] = stream.flatMap(new MyFlatMapFunction)

//    fm.print()

    env.execute()

  }

  class MyFlatMapFunction extends FlatMapFunction[String,String]{
    override def flatMap(t: String, collector: Collector[String]): Unit = {
      if (t.equals("white")){
        collector.collect(t)
      }else if (t.equals("black")){
        collector.collect(t)
        collector.collect(t)
      }
    }
  }

}
