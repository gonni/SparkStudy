package com.yg.streaming

import org.slf4j.LoggerFactory

object CrawledNewsProcessing extends SparkStreamingInit {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print
    wordCounts.foreachRDD((a, b) => {logger.info(a + "=>" + b)})

    ssc.start()
    ssc.awaitTermination()

  }
}
