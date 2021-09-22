package com.lgt.scala.sql

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}


object NonTypeCustomizeDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("NonTypeCustomizeDemo").master("local[2]").getOrCreate()
    session.udf.register("myAverage",MyAverage)
    val df = session.read.json("hdfs://192.168.32.28:8020/bigdata/json/emp.json")
    df.createOrReplaceTempView("emp")
    val myavg = session.sql("select myAverage(sal) as avg_sal from emp").first()
    val avg = session.sql("select avg(sal) as avg_sal from emp").first()

    println(myavg)
    println(avg)
  }
}

object MyAverage extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("inputColumn",LongType)::Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("sum",LongType) :: StructField("count",LongType)::Nil)
  }

  override def dataType: DataType =DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(0)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0)=buffer.getLong(0)+input.getLong(0)
      buffer(1)=buffer.getLong(1)+1
    }
  }


  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble/buffer.getLong(1)
}

