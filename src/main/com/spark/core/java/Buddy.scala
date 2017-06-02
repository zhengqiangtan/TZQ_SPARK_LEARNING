package com.spark.core.java

/**
  * Created by Administrator on 2017/6/1 0001.
  */
class Buddy {
  def greet(): Unit ={
    println("hello  from buddy class")
  }
}

object Buddy{
  def greet(): Unit ={
    println("hello form buddy object")
  }
}

//new Buddy ().greet ()
//Buddy$.MODULE$.greet ()
