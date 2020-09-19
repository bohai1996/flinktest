package com.bohai.dsapi

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random


class SensorSource extends RichParallelSourceFunction[SensorReading]{

  // 表示数据源是否正常运行
  var runnig = true

  // 上下文参数用来发出数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    val rand = new Random()

    var curFTemp = (1 to 10).map(
      // 使用高斯噪声产生随机温度值
      i => ("sensor_"+i,(rand.nextGaussian() * 20))
    )

    // 产生无限数据流
    while (runnig){
      curFTemp = curFTemp.map(
        t => (t._1,t._2 + (rand.nextGaussian()*0.5))
      )
       // 产生ms为单位时间戳
      val curTime = Calendar.getInstance().getTimeInMillis
      // 使用sourceContext的参数collect方法发射传感器数据
      curFTemp.foreach(
        t=>sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )
      // 每隔1000ms 发射一条数据
      Thread.sleep(1000)
    }

  }
  // 定义当取消flink任务时，需要关闭数据源
  override def cancel(): Unit = runnig = false
}
