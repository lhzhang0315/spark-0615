package com.atguigu.test

import com.atguigu.receiver.MyReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiveWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("RDDWordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.加载数据创建DStream
    val DStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))

    //4.打印
    DStream.print

    //5.开启任务
    ssc.start()
    ssc.awaitTermination()


  }

}
