package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序[scala]
  * Created by TZQ on 2016/12/8.
  */
object SecondSortScala {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("WordCountScala").setMaster("local")
    val sc=new SparkContext(conf)

    sc.textFile("c:\\sort.txt",1)
      .map(line=>(new SecondSortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))
      .sortByKey(true,1)
      .map(v=>v._2)
      .foreach(println)
  }
}
