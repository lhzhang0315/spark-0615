package com.atguigu.app

import com.atguigu.udaf.MyAvg
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TestSparkSQL {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TestSparkSQL").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    //3.创建SparkSession
    //    val spark = new SparkSession(sc)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestSparkSQL")
      .master("local[*]")
      .getOrCreate()

    //4.读取JSON文件创建DF
    val df: DataFrame = spark.read.json("C:\\Users\\WHAlex\\Desktop\\people.json")

    //5.DSL风格
    println("*************DSL*************")
//    df.select("name").show()

    //6.SQL风格
    println("*************SQL*************")
    df.createTempView("t1")

    //注册函数
    spark.udf.register("MyAVG", new MyAvg)

    spark.sql("select MyAVG(age) from t1").show()

    //7.关闭连接
    spark.stop()
    sc.stop()

  }

}
