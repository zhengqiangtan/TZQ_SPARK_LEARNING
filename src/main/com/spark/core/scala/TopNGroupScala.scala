package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组取TOPN [SCALA]
  * Created by TZQ on 2016/12/9.
  */
object TopNGroupScala {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TopNGroupScala").setMaster("local")
    val sc=new SparkContext(conf)

    sc.textFile("c:\\spark\\score.txt",1)
      .map(w=>(w.split(" ")(0),w.split(" ")(1).toInt)).groupByKey(1)
      .map{
        line=>(line._1,line._2.toList.sortWith(_ > _).take(3))
      }
      .foreach(println)
    sc.stop()
  }
}
//(class1,List(95, 90, 87))
//(class2,List(88, 87, 77))