package com.lgt.scala.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinDemo {
  private val session: SparkSession = SparkSession.builder().appName("JoinDemo").master("local[2]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val empdf: DataFrame = session.read.json("hdfs://192.168.32.28:8020/bigdata/json/emp.json")
    empdf.createOrReplaceTempView("emp")

    val deptdf = session.read.json("hdfs://192.168.32.28:8020/bigdata/json/dept.json")
    deptdf.createOrReplaceTempView("dept")

    val joinExpression = empdf.col("deptno") === deptdf.col("deptno")

    /*empdf.join(deptdf,joinExpression).select("ename","dname").show()
    session.sql("select ename,dname from emp join dept on emp.deptno=dept.deptno").show()*/

    /*empdf.join(deptdf,joinExpression,"outer").show()
    session.sql("select * from emp full outer join dept on emp.deptno =dept.deptno").show()*/

    /*empdf.join(deptdf,joinExpression,"left_outer").show()
    session.sql("select * from emp left outer join dept on emp.deptno=dept.deptno").show()*/

    /*empdf.join(deptdf,joinExpression,"right_outer").show()
    session.sql("select * from emp right outer join dept on emp.deptno=dept.deptno").show()*/

    /*empdf.join(deptdf,joinExpression,"left_semi").show()
    session.sql("select * from emp left semi join dept on emp.deptno=dept.deptno").show()*/

//    empdf.join(deptdf,joinExpression,"left_anti").show()
//    session.sql("select * from emp left anti join dept on emp.deptno=dept.deptno").show()

//    empdf.join(deptdf,joinExpression,"cross").show()
//    session.sql("select * from emp cross join dept on emp.deptno=dept.deptno").show()

    session.sql("select * from emp natural join dept").show()

  }

}
