package com.spark.core.scala

/**
  * 正则表达式
  * Created by Administrator on 2017/5/31 0031.
  */
object RegularExpr extends App {
  val pattern ="(S|s)cala".r
  val str="Scala is scalable and cool language"
  println(pattern findFirstIn  str)
  println(pattern findAllIn   str mkString(","))
  println("cool".r replaceFirstIn(str,"awesome"))
}
