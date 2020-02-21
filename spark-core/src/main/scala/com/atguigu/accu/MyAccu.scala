package com.atguigu.accu

import java.util

import org.apache.spark.util.AccumulatorV2

class MyAccu extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  //判断当前累加器是否为空
  override def isZero: Boolean = {
    println(s"isZero****${Thread.currentThread().getName}*******")
    list.isEmpty
  }


  //复制一个新的累计器
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {

    println(s"copy****${Thread.currentThread().getName}*******")

    new MyAccu

  }

  //重置累计器
  override def reset(): Unit = list.clear()

  //给累加器添加值
  override def add(v: String): Unit = list.add(v)

  //将多个分区的数据进行合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {


    println(s"merge****${Thread.currentThread().getName}*******")
    this.list.addAll(other.value)

  }

  //获取累加器中的数据
  override def value: util.ArrayList[String] = list
}
