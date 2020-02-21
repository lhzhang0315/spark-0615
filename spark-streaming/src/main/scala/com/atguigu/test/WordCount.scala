package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    println("*****")

    //3.创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.将一行数据编程一个个的单词
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5.将单词转换为元组
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(x => {
      println("*****")
      (x, 1)
    })

    //6.按照单词分组，进行value的求和
    val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    //7.打印
    wordToCountDStream.print()
    wordToCountDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //创建连接
        iter.foreach(x => println("使用连接"))
        //关闭连接
      })
    })

    //8.开启任务
    ssc.start()
    ssc.awaitTermination()

    ssc.stop()
  }
}