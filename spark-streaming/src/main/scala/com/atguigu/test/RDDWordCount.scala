package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("RDDWordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.通过加载RDD的数据形成DStream
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val RDDDStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    //4.累加求和
    val sumDStream: DStream[Int] = RDDDStream.reduce(_ + _)

    //5.打印
    sumDStream.print

    //6.开启任务
    ssc.start()

    //7.向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 100, 10)
      Thread.sleep(2000)
    }


    //8.阻塞Main线程
    ssc.awaitTermination()

  }

}
