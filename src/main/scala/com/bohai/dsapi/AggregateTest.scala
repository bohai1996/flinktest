package com.bohai.dsapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._

object AggregateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input: DataStream[(Int, String, Double)] = env.fromElements(
      (1, "hello", 4.0),
      (1, "hello", 5.0),
      (1, "hello", 5.0),
      (3, "world", 6.0),
      (3, "world", 6.0)
    )

    input.keyBy(_._1)

  }
}
