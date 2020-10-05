package com.bohai.dsapi

import org.apache.flink.streaming.api.scala._

object RedistributeExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.addSource(new SensorSource).setParallelism(1)
      .map(r=>r.id).setParallelism(1)

    env.execute()
  }

}
