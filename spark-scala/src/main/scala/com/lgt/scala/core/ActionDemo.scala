package com.lgt.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object ActionDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //reduceDemo(sc)
    //takeOrderedDemo(sc)
    //countByKeyDemo(sc)
    saveAsTextFile(sc)
  }

  def reduceDemo(sparkContext: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5)
    val i = sparkContext.parallelize(list).reduce((x, y) => x + y)
    val i1 = sparkContext.parallelize(list).reduce(_ + _)
  }

  def takeOrderedDemo(sparkContext: SparkContext): Unit = {
    val list = List((1, "hadoop"), (1, "storm"), (1, "azkaban"), (1, "hive"))

    implicit val implicitOrdering = new CustomOrdering

    sparkContext.parallelize(list).takeOrdered(5).foreach(println)
  }

  class CustomOrdering extends Ordering[(Int, String)] {

    override def compare(x: (Int, String), y: (Int, String)): Int = if (x._2.length > y._2.length) 1 else -1

  }

  def countByKeyDemo(sparkContext: SparkContext): Unit = {
    val list = List(("hadoop", 10), ("hadoop", 10), ("storm", 3), ("storm", 3), ("azkaban", 1))
    sparkContext.parallelize(list).countByValue().foreach(println)
  }

  def saveAsTextFile(sparkContext: SparkContext): Unit = {
    val list = List(("hadoop", 10), ("hadoop", 10), ("storm", 3), ("storm", 3), ("azkaban", 1))
    sparkContext.parallelize(list).saveAsTextFile("hdfs://192.168.32.28:8020/bigdata/spark_saveAsTextFile")
  }
}
