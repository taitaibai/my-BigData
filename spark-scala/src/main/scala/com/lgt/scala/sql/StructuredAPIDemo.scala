package com.lgt.scala.sql

import org.apache.spark.sql.functions.{asc, col, column, desc, lit}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object StructuredAPIDemo {


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("Spark-SQL").master("local[4]").getOrCreate()

    //externalDataSet(sparkSession)

    //innerDataSet(sparkSession)

    //rddToDataFrameDemo(sparkSession)

    //schemaDemo(sparkSession)

    columnsDemo(sparkSession)
  }

  def externalDataSet(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val ds = sparkSession.read.json("hdfs://192.168.32.28:8020/bigdata/emp.json").as[Emp]
    ds.show()
    ds
  }

  def innerDataSet(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val ds = Seq(Emp("ALLEN", 300.0, 30, 7499, "1981-02-20 00:00:00", "SALESMAN", 7698, 1600.0),
      Emp("JONES", 300.0, 30, 7499, "1981-02-20 00:00:00", "SALESMAN", 7698, 1600.0)).toDS()
    ds.show()
    ds
  }

  def rddToDataFrameDemo(sparkSession: SparkSession)= {
    import sparkSession.implicits._
    val ds = sparkSession.sparkContext.textFile("hdfs://192.168.32.28:8020/bigdata/dept.txt")
      .map(_.split("\t"))
      .map(line => Dept(line(0).trim.toLong, line(1), line(2)))
      .toDS()
    ds.show()
    ds
  }

  def schemaDemo(sparkSession: SparkSession) = {
    val fields = Array(StructField("deptno", LongType, nullable = true),
      StructField("dname", StringType, nullable = true),
      StructField("loc", StringType, nullable = true))

    val scheme = StructType(fields)

    val deptRDD = sparkSession.sparkContext.textFile("hdfs://192.168.32.28:8020/bigdata/dept.txt")
    val rowRDD = deptRDD.map(_.split("\t")).map(line => Row(line(0).toLong, line(1), line(2)))

    val deptDF = sparkSession.createDataFrame(rowRDD, scheme)
    //deptDF.show()
    scheme
  }

  def columnsDemo(sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    val df = externalDataSet(sparkSession).toDF()
    col("colName")
    column("colName")

    df.select($"ename",$"job").show()
    df.select('ename,'job).show()
    df.filter($"sal">2000).show()
    df.orderBy(desc("deptno"),asc("sal")).show()
    df.orderBy(desc("sal")).limit(3).show()
    df.select("deptno").distinct().show()
    df.groupBy("deptno").count().show()
    //df.withColumn("upSal",$"sal"+1000).show()
    //df.withColumn("intCol",lit(1000)).show()

    //df.drop("comm","job").show()
    //df.withColumnRenamed("comm","common").show()

  }

  case class Emp(ename: String, comm: Double, deptno: Long, empno: Long, hiredate: String, job: String, mgr: Long, sal: Double)

  case class Dept(deptno: Long, dname: String, loc: String)
}
