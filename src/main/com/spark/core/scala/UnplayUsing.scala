package com.spark.core.scala

/**
  * 提取器的使用2--提取多个值
  * Created by Administrator on 2017/6/6 0006.
  */
object UnplayUsing2 extends App {


  val user: User = new FreeUser("Daniel", 3000, 0.8d)
  user match {
    case FreeUser(name, _, p) =>
      if (p > 0.75) println(s"$name, what can we do for you today?")
      else println(s"Hello $name")
    case PremiumUser(name, _) => println( s"Welcome back, dear $name")

  }

  trait User {
    def name: String
    def score: Int
  }

  class FreeUser(
                  val name: String,
                  val score: Int,
                  val upgradeProbability: Double
                ) extends User

  class PremiumUser(
                     val name: String,
                     val score: Int
                   ) extends User

  object FreeUser {
    def unapply(user: FreeUser): Option[(String, Int, Double)] = Some((user.name, user.score, user.upgradeProbability))
  }

  object PremiumUser {
    def unapply(user: PremiumUser): Option[(String, Int)] = Some((user.name, user.score))
  }

}