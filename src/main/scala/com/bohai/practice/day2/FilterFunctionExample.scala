package com.bohai.practice.day2

import org.apache.flink.api.common.functions._
import org.apache.flink.streaming.api.scala._

object FilterFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.filter(r=>r.temperature > 0.0)

    stream.filter(new MyFilterFunction)

    stream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = t.temperature > 0.0
    }).print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = t.temperature > 0.0
  }


}
