package com.atguigu.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MyReceiver(hostName: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //接收数据并将数据发送给Spark集群
  def receive(): Unit = {

    //创建输入流
    try {
      val socket = new Socket(hostName, port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      //读取数据
      var input: String = reader.readLine()

      //将数据写给spark集群
      while (input != null && !isStopped()) {
        store(input)
        input = reader.readLine()
      }

      reader.close()
      socket.close()
      restart("重启。。。。")
    } catch {
      case e: java.net.ConnectException => restart("连接异常，重启。。。")
      case t: Throwable => restart("错误，重启。。。")
    }

  }


  override def onStart(): Unit = {

    //开启新的线程
    new Thread() {

      //重写run方法
      override def run(): Unit = {
        receive()
      }
    }.start()

  }

  override def onStop(): Unit = {

  }
}
