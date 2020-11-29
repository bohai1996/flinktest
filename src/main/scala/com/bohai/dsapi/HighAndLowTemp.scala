package com.bohai.dsapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighAndLowTemp {

  case class MinMaxTemp(id:String,min:Double,max:Double,endTs:Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val tmp: WindowedStream[SensorReading, String, TimeWindow] = stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))

    tmp.aggregate(new HighAndLowAgg,new WindowResult).print()

    env.execute()

  }

  class HighAndLowAgg extends AggregateFunction[SensorReading,(String,Double,Double),(String,Double,Double)]{
    // 最小温度值的初始值是Double的最大值
    // 最大温度值的初始值是Double的最小值
    override def createAccumulator(): (String, Double, Double) =("",Double.MaxValue,Double.MinValue)

    override def add(in: SensorReading, acc: (String, Double, Double)): (String, Double, Double) = {
      (in.id,in.temperature.min(acc._2),in.temperature.max(acc._3))
    }

    override def getResult(acc: (String, Double, Double)): (String, Double, Double) = acc

    override def merge(acc: (String, Double, Double), acc1: (String, Double, Double)): (String, Double, Double) = {
      (acc._1,acc._2.min(acc1._2),acc._3.max(acc1._3))
    }
  }

  class WindowResult extends ProcessWindowFunction[(String,Double,Double),MinMaxTemp,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val  minMax = elements.head
      out.collect(MinMaxTemp(key,minMax._2,minMax._3,context.window.getEnd))
    }
  }


}
