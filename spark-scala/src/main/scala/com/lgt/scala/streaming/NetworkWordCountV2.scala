package com.lgt.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountV2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","root")

    val sparkConf = new SparkConf().setAppName("NetworkWordCountV2").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("hdfs://192.168.32.28:8020/spark-streaming")

    val lines = ssc.socketTextStream("192.168.32.28", 9999)
    lines.flatMap(_.split(" ")).map(x=>(x,1)).updateStateByKey[Int](updateFunction _).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int],preValues: Option[Int])={
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current+pre)
  }

}
