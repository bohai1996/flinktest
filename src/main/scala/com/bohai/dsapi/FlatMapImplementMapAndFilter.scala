package com.bohai.dsapi

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapImplementMapAndFilter {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    // 使用flatMap 实现map功能，将传感器数据的id抽取出来
    stream
      .flatMap(new FlatMapFunction[SensorReading,String] {
        override def flatMap(t: SensorReading, collector: Collector[String]): Unit = {
          collector.collect(t.id)
        }
      }).print()

    // 使用flatMap实现filter功能，将 sensor_1 的数据过滤出来
    stream
        .flatMap(new FlatMapFunction[SensorReading,SensorReading] {
          override def flatMap(t: SensorReading, collector: Collector[SensorReading]): Unit = {
            if (t.id.equals("sensor_1")){
              collector.collect(t)
            }
          }
        })
        .print()

    env.execute()

  }

}
