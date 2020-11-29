package com.bohai.practice.day2

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
      .filter(r=>r.id.equals("sensor_1"))

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(r=>r.id)


    val maxStreams: DataStream[SensorReading] = keyedStream.max(2)
    maxStreams.print()

    env.execute()
  }

}
