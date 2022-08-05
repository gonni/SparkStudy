package com.yg.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import slick.jdbc.MySQLProfile.api._
import com.mchange.v2.c3p0.ComboPooledDataSource

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class MySqlSourceReceiver() extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)
//class MySqlSourceReceiver() extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)
  with Logging{
//  val cpdf = new ComboPooledDataSource
//  var db : Database = Database.forDataSource(cpdf, None)
//  val db : Database = Database.forURL("jdbc:mysql://root:18651865@localhost:3306/horus?enabledTLSProtocols=TLSv1.2", "com.mysql.jdbc.Driver")

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
//      store("AA BB CC A B C")
      Thread.sleep(1000)

      DbUtil.sample.foreach(dt => {
        println(dt.getOrElse("NULL"))
        store(dt.getOrElse("NULL"))
      })

//      val res = Await.result(db.run(CrawledRepo.findLatestAnchor(9L).result), 10.seconds)
//      res.foreach(anchor => {
//        println("stream source producing .. " + anchor)
//        store(anchor.getOrElse("NULL"))
//      })

    }
  }
}
