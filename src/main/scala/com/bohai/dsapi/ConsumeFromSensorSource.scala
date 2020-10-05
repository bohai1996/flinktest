package com.bohai.dsapi

import org.apache.flink.streaming.api.scala._

object ConsumeFromSensorSource {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 调用addSource方法
    val stream =  env.addSource(new SensorSource)
    stream.print()

    env.execute()
  }

}
