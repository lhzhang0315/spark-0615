package com.atguigu.bc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BCTest {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("MysqlTest").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义广播变量
    val bc: Broadcast[String] = sc.broadcast("aaa")

    //4.创建RDD
    val word: RDD[String] = sc.parallelize(Array("hello", "atguigu", "aaaaaa"))

    //5.使用广播变量
    word.filter(x => {

      //获取广播变量
      val value: String = bc.value

      x.contains(value)
    }).foreach(println)

    //6.关闭资源
    sc.stop()

  }
}
