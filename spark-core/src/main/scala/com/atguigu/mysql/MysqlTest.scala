package com.atguigu.mysql

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlTest {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("MysqlTest").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //MySQL参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    //3.读取MySQL数据创建RDD
    val rdd: JdbcRDD[Int] = new JdbcRDD[Int](
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select id,name from rddTable where ?<=id and id <=?;",
      1,
      10,
      1,
      result => result.getInt(2))

    //4.打印数据
    rdd.foreachPartition(iter => {
      //获取连接

      iter.foreach(x => {
        //使用连接
      })
      //关闭连接
    })

    rdd.foreach(iter => {

      //获取连接

      //使用连接

      //关闭连接
    })

    //5.关闭资源
    sc.stop()

  }


}
