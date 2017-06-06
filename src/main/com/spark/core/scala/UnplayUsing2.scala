package com.spark.core.scala

/**
  * 提取器的使用
  *
  * 在Scala中， Option 是 null 值的安全替代。 unapply 方法要么返回 Some[T] （如果它能成功提取出参数），要么返回 None ，
  * None 表示参数不能被 unapply 具体实现中的任一提取规则所提取出。
  *
  * Created by Administrator on 2017/6/6 0006.
  */
object UnplayUsing extends App{

  println(FreeUser.unapply(new FreeUser("Daniel")))

  val user: User = new PremiumUser("Daniel")
  user match {
    case FreeUser(name) => println("Hello" + name)
    case PremiumUser(name) => println("Welcome back, dear " + name)
  }

}

trait User {
  def name: String
}
class FreeUser(val name: String) extends User
class PremiumUser(val name: String) extends User

//提取一个参数
object FreeUser {
  def unapply(user: FreeUser): Option[String] = Some(user.name)
}
object PremiumUser {
  def unapply(user: PremiumUser): Option[String] = Some(user.name)
}
