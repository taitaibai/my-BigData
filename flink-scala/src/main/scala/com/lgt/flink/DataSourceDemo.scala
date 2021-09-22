package com.lgt.flink

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment



object DataSourceDemo {
  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    //env.readTextFile("/home/lgt/program/data/bigdata/wordcount.txt").print()
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.fromElements((1,2,3,4,5))

    senv.execute()
  }
}
