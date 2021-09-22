package com.lgt.scala.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator


object CustomizeDemo{
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("CustomizeDemo").master("local[2]").getOrCreate()

    import session.implicits._

    val ds = session.read.json("hdfs://192.168.32.28:8020/bigdata/json/emp.json").as[Emp]

    val myAvg = ds.select(CustAverage.toColumn.name("average_sal")).first()
    val avg = ds.select(functions.avg(ds.col("sal"))).first().get(0)
    println(myAvg)
    println(avg)
  }
}


case class Emp(ename: String, comm: scala.Option[Double], deptno: Long, empno: Long,
               hiredate: String, job: String, mgr: scala.Option[Long], sal: Double)

case class SumAndCount(var sum:Double,var count:Long)

object CustAverage extends Aggregator[Emp,SumAndCount,Double]{

  override def zero: SumAndCount = SumAndCount(0,0)

  override def reduce(avg: SumAndCount, emp: Emp): SumAndCount = {
    avg.sum +=emp.sal
    avg.count +=1
    avg
  }

  override def merge(avg1: SumAndCount, avg2: SumAndCount): SumAndCount = {
    avg1.sum +=avg2.sum
    avg1.count+=avg2.count
    avg1
  }

  override def finish(reduction: SumAndCount): Double = reduction.sum /reduction.count

  override def bufferEncoder: Encoder[SumAndCount] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
