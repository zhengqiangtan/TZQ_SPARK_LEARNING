package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计文件内单词数
  * Created by TZQ on 2016/12/9.
  */
object CountWords {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Top3Scala").setMaster("local")
    val sc=new SparkContext(conf)

    val sum=sc.textFile("c:\\spark\\spark.txt").map(w=>w.length).reduce(_+_)
    println("count words:"+sum)
  }
}
//count words:1835