package com.atguigu.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionerTest {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    //3.创建PairRDD
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("a", 2), ("b", 3), ("b", 4)))

    //4.修改分区规则重新分区
    val parRDD: RDD[(String, Int)] = rdd.partitionBy(new MyPartitioner(2))

    //5.查看结果
    parRDD.mapPartitionsWithIndex((index, items) => {

      items.map(x => (index, x))

    }).foreach(println)

    //6.关闭连接
    sc.stop()

  }

}
