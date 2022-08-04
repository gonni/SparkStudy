package com.yg.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MySqlSourceReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging{

  override def onStart(): Unit = {
    new Thread("MysqlSt") {
      override def run(): Unit = {
        createGetData
      }
    }.start()
  }

  override def onStop(): Unit = ???

  private def createGetData(): Unit = {

  }
}
