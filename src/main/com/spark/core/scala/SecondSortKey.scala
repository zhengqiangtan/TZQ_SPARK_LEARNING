package com.spark.core.scala

/**
  * 自定义二次排序key[scala]
  * Created by TZQ on 2016/12/8.
  */
class SecondSortKey(val first:Int,val second:Int)extends Ordered[SecondSortKey] with Serializable{
  override def compare(that: SecondSortKey): Int = {
    if (this.first-that.first!=0){
      this.first-that.first
    }else{
      this.second-that.second
    }
  }
}
