package com.yg.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import slick.jdbc.MySQLProfile.api._
import com.mchange.v2.c3p0.ComboPooledDataSource

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class MySqlSourceReceiver() extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)
  with Logging {

  override def onStart(): Unit = {
    new Thread("MysqlSt") {
      override def run(): Unit = {
        createGetData
      }
    }.start()
  }

  override def onStop(): Unit = synchronized {
//    this.db.close()
  }

  private def createGetData(): Unit = {
    while(!isStopped) {
      Thread.sleep(5000)
      DbUtil.getLatestAnchor.foreach(dt => {
//        println(dt.getOrElse("NULL"))
        store(dt.getOrElse("NULL"))
      })
    }
  }
}
