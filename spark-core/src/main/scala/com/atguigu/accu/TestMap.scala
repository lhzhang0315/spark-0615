package com.atguigu.accu

import scala.collection.mutable

object TestMap {

  def main(args: Array[String]): Unit = {

    val map = new mutable.HashMap[String, Long]()

    map("a") = 1L
    map("b") = 2L

    println(map)

  }

}
