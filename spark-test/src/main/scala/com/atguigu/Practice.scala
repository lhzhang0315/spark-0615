package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Practice {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    //3.读取数据:1516609143869 1 7 87 12
    val lineRDD: RDD[String] = sc.textFile("C:\\Users\\WHAlex\\Desktop\\agent.log")

    //4.获取省份&广告并赋值1
    val proAdToOne: RDD[((String, String), Int)] = lineRDD.map { line =>
      val strings: Array[String] = line.split(" ")
      ((strings(1), strings(4)), 1)
    }

    //5.计算每个省份每个广告被点击的总次数
    val proAdToCount: RDD[((String, String), Int)] = proAdToOne.reduceByKey(_ + _)

    //6.转换维度
    //    proAdToCount.map(x => (x._1._1, (x._1._2, x._2)))
    val proToAdCount: RDD[(String, (String, Int))] = proAdToCount.map { case ((pro, ad), count) => (pro, (ad, count)) }

    //7.按照省份分组
    val proToAdCountIter: RDD[(String, Iterable[(String, Int)])] = proToAdCount.groupByKey()

    //8.分组内部排序并取前三
    val result: RDD[(String, List[(String, Int)])] = proToAdCountIter.mapValues(iter => {

      iter.toList.sortWith(_._2 > _._2).take(3)

    })

    //9.打印数据
    result.foreach(println)

    //10关闭资源
    sc.stop()

  }

}
