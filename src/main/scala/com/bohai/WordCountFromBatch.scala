package com.bohai

import com.bohai.Job1.WordWithCount
import org.apache.flink.streaming.api.scala._

object WordCountFromBatch {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      "hello world",
      "aaa bbb",
      "aaa ccc"
    )

    val transformed = stream
      .flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w,1))
      .keyBy(0)
      .sum(1)

    transformed.print()

    env.execute()

  }

}
