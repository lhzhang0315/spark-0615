package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.读取文件
    val line: RDD[String] = sc.textFile("C:\\Users\\WHAlex\\Desktop\\input")

    //4.压平
    val word: RDD[String] = line.flatMap(x => x.split(" "))

    //5.将单词映射为元组
    val wordToOne: RDD[(String, Int)] = word.map(x => (x, 1))

    //6.求总和
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //7.保存文件
    wordToCount.saveAsTextFile("C:\\Users\\WHAlex\\Desktop\\out1")

    //8.关闭连接
    sc.stop()

  }

}