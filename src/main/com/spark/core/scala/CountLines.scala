package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计每行出现的次数
  * Created by TZQ on 2016/12/12.
  */
object CountLines {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("CountLines").setMaster("local")
    val sc=new SparkContext(conf)

    val lines=sc.textFile("c:\\spark\\hello.txt",1)
        lines.map(w=>(w,1))
          .reduceByKey(_+_)
          .foreach(println)

    sc.stop()

  }
}
//  (hello tan,2)
//  (hello boy,1)
//  (hello world,3)
//  (hello unionpay,1)
//  (hello morning,2)