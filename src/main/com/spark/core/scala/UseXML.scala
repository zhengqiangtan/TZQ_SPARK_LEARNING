package com.spark.core.scala

/**
  * Created by Administrator on 2017/6/1 0001.
  */
object UseXML extends App{
  val xmlFragment=
    <symbols>
      <symbol ticker="AAPL"><units>200</units></symbol>
      <symbol ticker="IBM"><units>215</units></symbol>
    </symbols>

  println(xmlFragment)
  println(xmlFragment.getClass)

  println("---------------------------------")



}
