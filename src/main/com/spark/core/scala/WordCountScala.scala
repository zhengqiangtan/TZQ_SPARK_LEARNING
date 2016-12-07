package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 案例一：统计单词频次并按照从高到低顺序输出
  * Created by TZQ on 2016/12/7.
  */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("WordCountScala").setMaster("local")
    val sc=new SparkContext(conf)

    val lines=sc.textFile("c:\\spark.txt",1)

    lines.flatMap(line=>line.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
      .map(w=>(w._2,w._1))
      .sortByKey(false,1)
      .map(w=>(w._2,w._1))
      .foreach(p => println("单词："+p._1+" 频次："+p._2))

  }
}

/*
单词：of 频次：10
单词：Spark 频次：10
单词：to 频次：10
单词：the 频次：7
单词： 频次：7
单词：and 频次：7
单词：our 频次：5
*/
