package com.atguigu.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AutoCommitDirectAPITest2 {

  def getSSC: StreamingContext = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverAPITest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.设置CheckPoint
    ssc.checkpoint("./ck1")

    //4.Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")

    //5.读取Kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaPara,
      Set("first"))

    //6.WordCount并打印
    kafkaDStream
      .flatMap { case (key, value) => value.split(" ") }
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //7.返回
    ssc
  }

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => getSSC)

    //2.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
