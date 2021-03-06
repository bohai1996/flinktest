package com.bohai.practice.day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MultiStreamTransformations {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val keyedTempStream = env.addSource(new SensorSource).keyBy(r => r.id)
    val smokeLevelStream = env.fromElements("LOW","HIGH").setParallelism(1)

    keyedTempStream
      .connect(smokeLevelStream.broadcast)
      .flatMap(new RaiseAlertFlatMap)
      .print()

    env.execute()
  }

  class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading,String,Alert]{
    private var smokeLevel:String = "LOW"
    override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
      if (smokeLevel == "HIGH" && in1.temperature > - 10000.0){
        collector.collect(Alert(in1.toString,in1.timestamp))
      }
    }

    override def flatMap2(in2: String, collector: Collector[Alert]): Unit = {
      smokeLevel = in2
    }
  }

}
