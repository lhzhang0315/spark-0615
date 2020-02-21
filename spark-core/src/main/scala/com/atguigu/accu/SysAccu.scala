package com.atguigu.accu

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SysAccu {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("MysqlTest").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.创建RDD
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9))

    //4.注册累加器
    val accu: LongAccumulator = sc.longAccumulator("accu")

    //5.遍历数据并使用累加器
    val value: RDD[Int] = rdd.map(x => {

      if (x > 2) {
        accu.add(1L)
      }
      x
    })

    value.collect()
    value.foreach(println)

    println("**************************")

    //6.取出累加器的值
    println(accu.value)

    //7关闭连接
    sc.stop()
  }


}
