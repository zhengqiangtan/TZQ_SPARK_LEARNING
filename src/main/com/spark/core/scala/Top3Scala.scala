package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对文本文件内的数字，取最大的前3个
  * Created by TZQ on 2016/12/9.
  */
object Top3Scala {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Top3Scala").setMaster("local")
    val sc=new SparkContext(conf)
    val rdd=sc.textFile("c:\\spark\\top.txt",1)
    rdd.map(w=>(w,w)).sortByKey(false,1).take(3).foreach(w=>println(w._2))

  }
}
//9
//7
//6