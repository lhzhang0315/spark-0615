package com.atguigu.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverAPITest {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverAPITest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      "zookeeper.connection" -> "",
      "groupId" -> "bigdata"
    )

    //4.读取Kafka数据创建流
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "bigdata",
      Map[String, Int]("first" -> 2))

    //5.打印数据
    kafkaDStream.print()

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()


  }

}
