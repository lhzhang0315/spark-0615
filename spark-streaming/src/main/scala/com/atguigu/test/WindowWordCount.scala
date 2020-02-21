package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WindowWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.开窗，窗口大小为2倍的批次大小，步长等于批次大小
    val windowDStream: DStream[String] = lineDStream.window(Seconds(15), Seconds(5))

    //5.计算WordCount
    val wordToCountDStream: DStream[(String, Int)] = windowDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //6.打印
    wordToCountDStream.print

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
