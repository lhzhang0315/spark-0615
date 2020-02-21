package com.atguigu.accu

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestMyAccu {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("MysqlTest").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.创建RDD
    val word: RDD[String] = sc.parallelize(Array("hello", "atguigu"))

    val myAccu = new MyAccu

    //4.注册累加器
    sc.register(myAccu, "myAccu")

    //5.使用累加器
    //    word.foreach(x => {
    //
    //      if (x.contains("a")) {
    //        myAccu.add(x)
    //      }
    //
    //      println(x)
    //    })

        word.map(x => {
          if (x.contains("a")) {
            myAccu.add(x)
          }
          x
        }).collect()

//    word.map(x => {
//      if (x.contains("a")) {
//        myAccu.add(x)
//      }
//      x
//    }).foreach(println)


    //6.取出累加器中的值
    val list: util.ArrayList[String] = myAccu.value

    import collection.JavaConversions._

    for (value: String <- list) {
      println(value)
    }

    //7.关闭资源
    sc.stop()

  }

}
