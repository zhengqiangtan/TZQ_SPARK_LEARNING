package com.spark.core.scala

/**
  * 高阶函数
  * Created by Administrator on 2017/5/24 0024.
  */
object GaojieFunction extends App{
  val array=Array(3,3,5,6,8)
  val sum=inject(array,0,(i,j)=>i+j)
  println(array.mkString+" is "+sum)

  val max=inject(array,Integer.MIN_VALUE,(a,b)=>Math.max(a,b))
  println("max:"+max)

  def inject(arr:Array[Int],initial:Int,operation:(Int,Int)=>Int):Int={
    var carryOver=initial
    arr.foreach(element=>carryOver=operation(carryOver,element))
    carryOver
  }
}
