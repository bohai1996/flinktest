package com.bohai.practice.day2
import java.util.Calendar

import scala.util.Random
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class SensorSource extends RichParallelSourceFunction[SensorReading]{

  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curFTemp = (1 to 10).map(i => ("sensor_" + i,rand.nextGaussian() * 20))

    // 无限发送数据
    while(running){
      curFTemp = curFTemp.map(t=>(t._1,t._2 + (rand.nextGaussian() * 0.5)))
      // 产生一个毫秒时间戳
      val curTime = Calendar.getInstance.getTimeInMillis

      curFTemp.foreach(t=>sourceContext.collect(SensorReading(t._1,curTime,t._2)))
      Thread.sleep(1000L)
    }
  }

  override def cancel(): Unit = ???
}
