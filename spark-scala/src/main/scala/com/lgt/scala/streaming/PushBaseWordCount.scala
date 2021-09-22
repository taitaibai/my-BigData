package com.lgt.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PushBaseWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(5))
    val flumeStream = FlumeUtils.createStream(ssc, "192.168.32.28", 8888)
    flumeStream.map(line=>new String(line.event.getBody.array()).trim).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
