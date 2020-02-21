package com.atguigu.search

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    val a = 0
    val b = 1
    val result = a + b
    println(result)

    //3.创建RDD
    val word: RDD[String] = sc.parallelize(Array("atguigu", "hello", "spark", "hadoop"))

    //4.构建Search对象
    val search = new Search("a")

    //5.过滤数据
    //    val filteredWord: RDD[String] = search.getMatch1(word)
    val filteredWord: RDD[String] = search.getMatch2(word)
    //    val filteredWord: RDD[String] = word.filter(_.contains("a"))

    //6.打印数据
    filteredWord.foreach(println)

    //7.关闭连接
    sc.stop()

  }

}
