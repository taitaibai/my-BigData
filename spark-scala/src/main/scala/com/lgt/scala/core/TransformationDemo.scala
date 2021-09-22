package com.lgt.scala.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TransformationDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //map(sc)
    //filter(sc)
    //flatMap(sc)
    //mapPartitions(sc)
    //mapPartitionsWithIndexDemo(sc)
    //sampleDemo(sc)
    //unionDemo(sc)
    //intersectionDemo(sc)
    //distinctDemo(sc)
    //groupByKeyDemo(sc)
    //reduceByKeyDemo(sc)
    //sortByKeyDemo(sc)
    //sortByDemo(sc)
    joinDemo(sc)
    //cogroupDemo(sc)
    //cartesianDemo(sc)
    aggregateByKeyDemo(sc)
  }

  def map(sc: SparkContext): Unit = {
    val list = List(1, 2, 3)
    sc.parallelize(list).map(_ * 10).foreach(println)
  }

  def filter(sc: SparkContext): Unit = {
    val list = List(3, 6, 9, 10, 12, 21)
    sc.parallelize(list).filter(_ >= 10).foreach(println)
  }

  def flatMap(sc: SparkContext): Unit = {
    /*val list=List(List(1,2),List(3),List(4,5))
    sc.parallelize(list).flatMap(_.toList).map(_*10).foreach(println)*/

    val lines = List("spark flume spark", "hadoop flume hive")
    sc.parallelize(lines).flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
  }

  def mapPartitions(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    sc.parallelize(list, 3).mapPartitions(iterator => {
      val buffer = new ListBuffer[Int]
      while (iterator.hasNext) {
        buffer.append(iterator.next() * 100)
      }
      buffer.toIterator
    }).foreach(println)
  }

  def mapPartitionsWithIndexDemo(sparkContext: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    sparkContext.parallelize(list, 3).mapPartitionsWithIndex((index, iterator) => {
      val buffer = new ListBuffer[String]
      while (iterator.hasNext) {
        buffer.append(index + "分区" + iterator.next() * 100)
      }
      buffer.toIterator
    }).foreach(println)
  }

  def sampleDemo(sparkContext: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    sparkContext.parallelize(list).sample(withReplacement = false, fraction = 0.5).foreach(println)
    println("############")
    list.foreach(println)
  }

  def unionDemo(sparkContext: SparkContext): Unit = {
    val list1 = List(1, 2, 3)
    val list2 = List(4, 5, 6)
    sparkContext.parallelize(list1).union(sparkContext.parallelize(list2)).foreach(println)
  }

  def intersectionDemo(sparkContext: SparkContext): Unit = {
    val list1 = List(1, 2, 3, 4, 5)
    val list2 = List(4, 5, 6)
    sparkContext.parallelize(list1).intersection(sparkContext.parallelize(list2)).foreach(println)
  }

  def distinctDemo(sparkContext: SparkContext): Unit = {
    val list = List(1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6)
    sparkContext.parallelize(list).distinct().foreach(println)
  }

  def groupByKeyDemo(sparkContext: SparkContext): Unit = {
    val list = List(("hadoop", 2), ("spark", 3), ("spark", 5), ("storm", 6), ("hadoop", 2))
    sparkContext.parallelize(list).groupByKey().map(x => (x._1, x._2.toList)).foreach(println)
  }

  def reduceByKeyDemo(sparkContext: SparkContext): Unit = {
    val list = List(("hadoop", 2), ("spark", 3), ("spark", 5), ("storm", 6), ("hadoop", 2))
    sparkContext.parallelize(list).reduceByKey(_ + _).foreach(println)
  }

  def sortByKeyDemo(sparkContext: SparkContext): Unit = {
    val list01 = List((100, "hadoop"), (90, "spark"), (120, "storm"))
    sparkContext.parallelize(list01).sortByKey(ascending = false).foreach(println)
  }

  def sortByDemo(sparkContext: SparkContext): Unit = {
    val list02 = List(("hadoop", 100), ("spark", 90), ("storm", 120))
    sparkContext.parallelize(list02).sortBy(x => x._2, ascending = false).foreach(println)
  }

  def joinDemo(sparkContext: SparkContext): Unit = {
    val list01 = List((1, "student01"), (2, "student02"), (3, "student03"))
    val list02 = List((1, "teacher01"), (2, "teacher02"), (3, "teacher03"))
    sparkContext.parallelize(list01).join(sparkContext.parallelize(list02)).foreach(println)
  }

  def cogroupDemo(sparkContext: SparkContext): Unit = {
    val list01 = List((1, "a"), (1, "a"), (2, "b"), (3, "e"))
    val list02 = List((1, "A"), (2, "B"), (3, "E"))
    val list03 = List((1, "[ab]"), (2, "[bB]"), (3, "eE"), (3, "eE"))
    sparkContext.parallelize(list01)
      .cogroup(sparkContext.parallelize(list02), sparkContext.parallelize(list03))
      .foreach(println)
  }

  def cartesianDemo(sparkContext: SparkContext): Unit = {
    val list1 = List("A", "B", "C")
    val list2 = List(1, 2, 3)
    sparkContext.parallelize(list1).cartesian(sparkContext.parallelize(list2)).foreach(println)
  }

  def aggregateByKeyDemo(sparkContext: SparkContext): Unit = {
    val list = List(("hadoop", 3), ("hadoop", 2), ("spark", 4), ("spark", 3), ("storm", 6), ("storm", 8))
    sparkContext.parallelize(list, numSlices = 2).aggregateByKey(zeroValue = 0, numPartitions = 3)(
      seqOp = _ + _,
      combOp = _ + _
    ).collect.foreach(println)
  }


}
