package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TransWordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    println(s"*****************${Thread.currentThread().getName}")

    //3.读取端口数据创建流
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.使用transform算子进行WordCount计算
    val wordToCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {

      println(s"----------------${Thread.currentThread().getName}")

      rdd.foreachPartition(iter => {
        //创建连接
        iter.foreach(
          //使用连接
          println
        )
        //关闭连接
      })

      rdd.flatMap(_.split(" ")).map(x => {
        println(s"++++++++++++++++++${Thread.currentThread().getName}")
        (x, 1)
      }).reduceByKey(_ + _)
    })

    //5.打印
    wordToCountDStream.print

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
