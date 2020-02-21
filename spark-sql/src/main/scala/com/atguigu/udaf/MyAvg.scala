package com.atguigu.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MyAvg extends UserDefinedAggregateFunction {

  //定义输入类型
  override def inputSchema: StructType = StructType(StructField("input", IntegerType) :: Nil)

  //中间缓存数据的类型
  override def bufferSchema: StructType = StructType(StructField("sum", IntegerType) :: StructField("count", IntegerType) :: Nil)

  //输出数据类型
  override def dataType: DataType = DoubleType

  //函数稳定性
  override def deterministic: Boolean = true

  //中间缓存的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  //区内累加数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getInt(0) + input.getInt(0)
    }

    buffer(1) = buffer.getInt(1) + 1

  }

  //区间累加数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //最终计算
  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0) / buffer.getInt(1).toDouble
  }
}
