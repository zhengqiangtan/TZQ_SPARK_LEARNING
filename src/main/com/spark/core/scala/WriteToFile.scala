package com.spark.core.scala

import java.io._

/**
  * loan模式（贷出模式）
  * 参考：http://haolloyin.blog.51cto.com/1177454/387138/
  * 对于那些资源密集（resource-intensive）型对象的使用应该使用这一模式。
  * Created by Administrator on 2017/5/24 0024.
  */
object WriteToFile extends App{
  writeToFile("demo.txt"){
    writer=> writer write "hello world"
  }


  def writeToFile(fileName:String)(codeBlock:PrintWriter=>Unit)={
    val writer=new PrintWriter(new File(fileName))
    try{
      codeBlock(writer)
    }finally {
      writer.close()
    }

  }
}
