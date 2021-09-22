package com.lgt.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetwordWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetwordWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("192.168.32.28", 9999)
    lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()

  }

}
