package com.bohai.dsapi

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {

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

    // 直接connect两条流没有任何意义
    // 必须要把相同key的流联合在一起处理
    val connected: ConnectedStreams[(String, Int), (String, Int)] = stream1
      .keyBy(_._1).connect(stream2.keyBy(_._1))

    val printed: DataStream[String] = connected.map(new MyCoMapFunction)

    printed.print()

    env.execute()

  }

  class MyCoMapFunction extends CoMapFunction[(String,Int),(String,Int),String]{
    // map1 处理来自第一条流的元素
    override def map1(in1: (String, Int)): String = {
       in1._1 + "的体重是："+in1._2+"斤"
    }
    // map2 处理来自第二条流的元素
    override def map2(in2: (String, Int)): String = {
      in2._1 + "的年龄是："+in2._2+"岁"
    }
  }

}
