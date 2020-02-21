package com.atguigu.app

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestMySQLRead {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestMySQL")
      .getOrCreate()

    //2.创建连接JDBC的参数
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "000000")

    //3.读取MySQL数据创建DF
    //    val jdbcDF: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop102:3306/rdd", "rddTable", properties)
    val jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/rdd")
      .option("dbtable", " rddTable")
      .option("user", "root")
      .option("password", "000000")
      .load()

    //4.打印数据
    jdbcDF.show()

    //5.关闭资源
    spark.stop()

  }

}
