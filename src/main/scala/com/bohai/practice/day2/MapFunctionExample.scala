package com.bohai.practice.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.map(r => r.id)

    stream.map(new IdExtractor)

    stream
      .map(new MapFunction[SensorReading,String] {
        override def map(t: SensorReading): String = t.id
      })
      .print()

    env.execute()

  }

  class IdExtractor extends MapFunction[SensorReading,String]{
    override def map(t: SensorReading): String = t.id
  }

}
