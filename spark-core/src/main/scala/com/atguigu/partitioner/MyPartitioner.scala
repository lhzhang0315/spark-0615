package com.atguigu.partitioner

import org.apache.spark.Partitioner

class MyPartitioner(numPar: Int) extends Partitioner {

  override def numPartitions: Int = numPar

  override def getPartition(key: Any): Int = {
    0
  }
}
