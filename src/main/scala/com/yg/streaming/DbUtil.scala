package com.yg.streaming
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object DbUtil {
  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global
  val db : Database = Database.forURL(url ="jdbc:mysql://192.168.35.123:3306/horus?useSSL=false",
      user="root", password="18651865", driver = "com.mysql.jdbc.Driver")

  val sample = Await.result(db.run(CrawledRepo.findLatestAnchor(21L).result), 10.seconds)

  val getLatestAnchor = Await.result(db.run(CrawledRepo.findLatestAnchor(21L).result), 10.seconds)

  def main(args: Array[String]): Unit = {
    sample.foreach(anchor => {
      println(anchor)
    })

  }
}
