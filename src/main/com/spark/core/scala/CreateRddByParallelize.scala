package com.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 并行化集合创建RDD，求累加和
  * Created by TZQ on 2016/12/12.
  */
object CreateRddByParallelize extends App{
  val conf=new SparkConf().setAppName("CreateRddByParallelize").setMaster("local")
  val sc=new SparkContext(conf)

  val sum=sc.parallelize(Array(1,2,3,4,5,6,6,7,8,9),1).reduce(_+_)
  println(sum)

}
