package com.atguigu.app

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TestRead {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TestSparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestSparkSQL")
      .master("local[*]")
      .getOrCreate()

    val df1: DataFrame = spark.read.json("./aa.json")
    val df2: DataFrame = spark.read.format("json").load("./aa.json")

    val properties = new Properties()
    val frame: DataFrame = spark.read.jdbc("url", "rddTable", properties)
    spark.read.format("jdbc").option("", "").load()

    df1.write.mode(SaveMode.Append).json("./bb.json")
    df1.write.format("json").save("./bb.json")

  }

}
