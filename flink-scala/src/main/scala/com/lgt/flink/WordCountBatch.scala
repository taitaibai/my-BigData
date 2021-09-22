package com.lgt.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation

object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    val ds = benv.readTextFile("/home/lgt/program/data/wordcount.txt")
    ds.flatMap{ _.toLowerCase.split(",")}
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
