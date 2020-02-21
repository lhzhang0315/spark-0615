package com.atguigu.app

import com.atguigu.bean.People
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TransRDDToDF {

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

    //导入隐式转换
    import spark.implicits._

    //4.创建RDD
    val line: RDD[String] = sc.textFile("C:\\Users\\WHAlex\\Desktop\\people.txt")

    //5.将RDD转换ROW类型
    val rowRDD: RDD[Row] = line.map { x =>
      val strings: Array[String] = x.split(",")
      Row(strings(0).trim, strings(1).trim.toInt)
    }

    //6.准备结构信息
    val structType = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)

    // 7.创建DF
    val df: DataFrame = spark.createDataFrame(rowRDD, structType)

    //8.打印
    df.show()

    //9.关闭资源
    spark.stop()


  }

}
