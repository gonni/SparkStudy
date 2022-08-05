package com.yg.streaming
import slick.jdbc.MySQLProfile.api._
object CrawledProcessing extends SparkStreamingInit {

  def main(args: Array[String]): Unit = {
    println("Active System ..")
    val db : Database = Database.forURL(
      url ="jdbc:mysql://localhost:3306/horus?enabledTLSProtocols=TLSv1.2",
      user="root",
      password="18651865",
      driver = "com.mysql.jdbc.Driver")

    val anchors = ssc.receiverStream(new MySqlSourceReceiver)

    val words = anchors.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print
    wordCounts.foreachRDD((a, b) => {println(a + "->" + b)})

    ssc.start()
    ssc.awaitTermination()
  }

}
