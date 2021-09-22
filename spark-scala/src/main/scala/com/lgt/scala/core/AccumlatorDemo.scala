package com.lgt.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object AccumlatorDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumlatorDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    //accumlatorDemo(sc,data)
    broadcastDemo(sc, data)
  }

  def accumlatorDemo(sparkContext: SparkContext, array: Array[Int]): Unit = {
    val accumulator = sparkContext.longAccumulator("My Accumulator")
    sparkContext.parallelize(array).foreach(x => accumulator.add(x))
    println(accumulator.value)
  }

  def broadcastDemo(sparkContext: SparkContext, array: Array[Int]): Unit = {

    val broadcastVar = sparkContext.broadcast(array)
    sparkContext.parallelize(broadcastVar.value).map(_ * 10).collect().foreach(println)

  }


}
