package com.lgt.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = senv.socketTextStream("192.168.32.28", 9999, '\n')
    ds.flatMap{ line => line.toLowerCase.split(",")}
      .filter(_.nonEmpty)
      .map{word=>(word,1)}
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)
      .print()
    senv.execute("streaming wordcount")

  }

}
