package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object UpdateStateWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TransWordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck5")

    //3.读取端口数据创建流
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.将一行数据转换为（单词，1）
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1))

    //5.有状态转换计算WordCount
    val updateStateFunc: (Seq[Int], Option[Int]) => Some[Int] = (seq: Seq[Int], state: Option[Int]) => {

      //a.当前批次累加求和
      val sum: Int = seq.sum

      //b.取出上一个批次的数据
      val lastSum: Int = state.getOrElse(0)

      //c.返回结果
      Some(sum + lastSum)
    }

    val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.updateStateByKey(updateStateFunc)

    //6.打印
    wordToCountDStream.print

    //7.启动任务
    ssc.start()
    ssc.awaitTermination()


  }

}
