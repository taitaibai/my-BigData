package com.lgt.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object NetWordCountToRedis {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("NetWordCountToRedis").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("192.168.32.28", 9999)
    val pairs = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
   /* pairs.foreachRDD(rdd =>
      rdd.foreachPartition({ partitionOfRecords =>
        var jedis: Jedis = null
        jedis = JedisPoolUtil.getConnection
        partitionOfRecords.foreach(record =>
          jedis.hincrBy("wordCount", record._1, record._2)
        )
        jedis.close()
      }))*/
    ssc.start()
    ssc.awaitTermination()
  }


}
