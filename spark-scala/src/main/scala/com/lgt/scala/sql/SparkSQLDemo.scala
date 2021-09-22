package com.lgt.scala.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SPARK-SQL").master("local[3]").getOrCreate()

    //sparkSQLDemo(sparkSession)

    //jsonDemo(sparkSession)

    //parquetDemo(sparkSession)

    //orcDemo(sparkSession)

    //mysqlDemo(sparkSession)

    aggregationDemo(sparkSession)
  }

  def aggregationDemo(sparkSession: SparkSession): Unit ={
    val df = jsonDemo(sparkSession)

    df.createOrReplaceTempView("emp")
    df.show()

    df.select(count("ename")).show()
    df.select(countDistinct("deptno")).show()
    df.select(approx_count_distinct("ename",0.1)).show()
    df.select(first("ename"),last("job")).show()
    df.select(sum("sal")).show()
    df.select(sumDistinct("sal")).show()
    df.select(avg("sal")).show()
    df.select(var_pop("sal"),var_samp("sal"),stddev("sal"),stddev("sal")).show()
    df.select(skewness("sal"),kurtosis("sal")).show()
    df.select(corr("empno","sal"),
      covar_samp("empno","sal"),
      covar_pop("empno","sal")).show()
    df.agg(collect_set("job"),collect_list("ename")).show()
    df.groupBy("deptno","job").count().show()
    sparkSession.sql("select deptno,job,count(*) from emp group by deptno,job").show()
    df.groupBy("deptno").agg(count("ename").alias("人数"),sum("sal").alias("总工资")).show()
    df.groupBy("deptno").agg("ename"->"count","sal"->"sum").show()
    sparkSession.sql("select deptno,count(ename),sum(sal) from emp group by deptno").show()

  }

  def sparkSQLDemo(sparkSession: SparkSession): Unit ={
    val df = StructuredAPIDemo.externalDataSet(sparkSession).toDF()
    df.createOrReplaceTempView("emp")

    sparkSession.sql("select ename,job from emp").show()

    sparkSession.sql("select * from emp where sal >2000").show()

    sparkSession.sql("select * from emp order by sal desc limit 3").show()

    sparkSession.sql("select * from emp order by deptno desc,sal asc").show()

    sparkSession.sql("select distinct(deptno) from emp").show()

    sparkSession.sql("select deptno,count(ename) from emp group by deptno").show()
  }

  def readCSVDemo(sparkSession: SparkSession): Unit ={
    val schame = new StructType(Array(
      StructField("deptno", LongType, nullable = false),
      StructField("dname", StringType, nullable = true),
      StructField("loc", StringType, nullable = true)
    ))

    sparkSession.read.format("csv")
      .option("mode","PERMISSIVE")
      .schema(schame)
      .load("hdfs://192.168.32.28:8020/bigdata/csv/dept.csv")
      .show()

    val df = sparkSession.read.format("csv")
      .option("mode", "PERMISSIVE")
      .schema(schame)
      .load("hdfs://192.168.32.28:8020/bigdata/csv/dept.csv")

    df.write.format("csv")
      .mode("overwrite")
      .option("sep","\t")
      .save("hdfs://192.168.32.28:8020/bigdata/csv/dept2.csv")
  }

  def jsonDemo(sparkSession: SparkSession) ={
    val df = sparkSession.read.format("json")
      .option("mode", "FAILFAST")
      .load("hdfs://192.168.32.28:8020/bigdata/json/emp.json")
    df.show(5)

    /*df.write.format("json")
      .mode("overwrite")
      .save("hdfs://192.168.32.28:8020/bigdata/json/dept2.json")*/
    df
  }

  def parquetDemo(sparkSession: SparkSession): Unit ={

    val df = sparkSession.read.format("parquet")
      .load("hdfs://192.168.32.28:8020/bigdata/parquet/dept.parquet")

    df.show(5)

    df.write.format("parquet")
      .mode("overwrite")
      .save("hdfs://192.168.32.28:8020/bigdata/parquet/dept2.parquet")

    sparkSession.read.format("parquet")
      .load("hdfs://192.168.32.28:8020/bigdata/parquet/dept2.parquet")
      .show()


  }

  def orcDemo(sparkSession: SparkSession): Unit ={
    val df = sparkSession.read.format("orc")
      .load("hdfs://192.168.32.28:8020/bigdata/orc/dept.orc")

    df.show(5)

    df.write.format("orc")
      .mode("overwrite")
      .save("hdfs://192.168.32.28:8020/bigdata/orc/dept2.orc")

    sparkSession.read.format("orc")
      .load("hdfs://192.168.32.28:8020/bigdata/orc/dept2.orc")
      .show()
  }

  def mysqlDemo(sp: SparkSession): Unit ={

    sp.read.format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://192.168.32.28:3306/mysql")
      .option("dbtable","help_keyword")
      .option("user","root")
      .option("password","root")
      .load()
      .show(10)
  }


  case class Emp(ename: String, comm: Double, deptno: Long, empno: Long, hiredate: String, job: String, mgr: Long, sal: Double)

  case class Dept(deptno: Long, dname: String, loc: String)

}
