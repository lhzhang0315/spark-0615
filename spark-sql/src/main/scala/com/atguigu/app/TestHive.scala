package com.atguigu.app

import org.apache.spark.sql.SparkSession

object TestHive {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestMySQLWrite")
      .enableHiveSupport()
      .getOrCreate()

    //2.创建表
    spark.sql("use gmall").show()
    spark.sql("show tables").show()

    //3.关闭资源
    spark.stop()

  }

}
