package com.yg.streaming
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object DbUtil {
  protected implicit def executor = scala.concurrent.ExecutionContext.Implicits.global
  val db : Database = Database.forURL(url ="jdbc:mysql://localhost:3306/horus?enabledTLSProtocols=TLSv1.2",
      user="root", password="18651865", driver = "com.mysql.jdbc.Driver")

  val sample = Await.result(db.run(CrawledRepo.findLatestAnchor(9L).result), 10.seconds)

  def main(args: Array[String]): Unit = {
    sample.foreach(anchor => {
      println(anchor)
    })

  }
}
