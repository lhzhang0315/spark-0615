package com.atguigu

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.SparkConf

object KafkaSparkTest {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("KafkaSparkTest").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //定义Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> "",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> ""
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("topic"), kafkaPara)
    )

    kafkaStream.print

  }

}