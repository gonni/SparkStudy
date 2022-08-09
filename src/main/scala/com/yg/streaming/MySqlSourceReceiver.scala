package com.yg.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import slick.jdbc.MySQLProfile.api._
import com.mchange.v2.c3p0.ComboPooledDataSource

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object DbUtil {
  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global
//  val db : Database = Database.forURL(url ="jdbc:mysql://192.168.35.123:3306/horus?useSSL=false",
  val db : Database = Database.forURL(url ="jdbc:mysql://localhost:3306/horus?useSSL=false",
    user="root", password="18651865", driver = "com.mysql.jdbc.Driver")

  val getLatestAnchorWithLimit = (seedNo: Long, startCrawlNo: Long, limit : Int) =>
    Await.result(db.run(CrawledRepo.findLatestAnchor(seedNo, startCrawlNo, limit).result), 10.seconds)

//  val getLatestAnchor = Await.result(db.run(CrawledRepo.findLatestAnchor(21L).result), 10.seconds)
  val getMaxCrawlNo = (seedNo: Long) => Await.result(db.run(CrawledRepo.findLatestCrawlNo(seedNo).result), 10.seconds).getOrElse(0L)

  def latestCrawlNo(seedNo: Long) : Long = {
    Await.result(db.run(CrawledRepo.findLatestCrawlNo(seedNo).result), 10.seconds).getOrElse(0L)
  }

  def getLatestAnchorFrom(seedNo: Long, startCrawlNo: Long) = {
    Await.result(db.run(CrawledRepo.findLatestAnchor(seedNo, startCrawlNo, 100).result), 10.seconds)
  }

  def getLatestContextFrom(seedNo: Long, startCrawlNo: Long) = {
    Await.result(db.run(CrawledRepo.findLatestContent(seedNo, startCrawlNo, 100).result), 10.seconds)
  }

//  val getLatestAnchorFrom = (startCrawlNo: Long) => Await.result(db.run(CrawledRepo.findLatestAnchor(21L).result), 10.seconds)

  def main(args: Array[String]): Unit = {
//    getLatestAnchorWithLimit(21L,1L,10).foreach(anchor => {
//      println(anchor)
//    })
    println("LatestCrawlNo ->" + getMaxCrawlNo(9L))

    println("Cont =>" + getLatestContextFrom(9L, 361830L).map(_.getOrElse("NULL")).mkString("\n\n"))
//      .foreach(a => {
//      println(a)
//    })

//    var target = Seq[Int](1,2,3,5,6)
//    target = target :+ 7
//    println(target)
  }
}

class MySqlSourceReceiver(val seedNo : Long) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)
  with Logging {

  var latestCrawlNo = 0L

//  def appedTargetSeed(seedId: Long) = {
//    targetSeeds = targetSeeds :+ seedId
//  }

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
      try {

//        val allCont = DbUtil.getLatestContextFrom(21L, latestCrawlNo).map(_.getOrElse("null")).mkString("\n\n")
//        println("crawled-->" + allCont)
//         store(allCont)

        val res = DbUtil.getLatestContextFrom(seedNo, latestCrawlNo)
        println(s"Count of crawled data : ${res.size} for ${seedNo}")
//        DbUtil.getLatestContextFrom(seedNo, latestCrawlNo).foreach(dt => {
//          println(dt.getOrElse("NULL"))
//          store(dt.getOrElse("NULL"))
//        })

        val mergedRes = DbUtil.getLatestContextFrom(seedNo, latestCrawlNo).mkString("\n\n")
        println(mergedRes)
        store(mergedRes)

        latestCrawlNo = DbUtil.latestCrawlNo(seedNo)
        println(s"Update Point ${latestCrawlNo}")

        Thread.sleep(5000)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
