package com.yg.streaming
import com.yg.tsdb.InfluxClient
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import kr.co.shineware.nlp.komoran.model.Token
import slick.jdbc.MySQLProfile.api._

import scala.collection.JavaConverters._
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


class HangleTokenizer extends Serializable {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  def arrayTokens(sentence : String) = {
    val tokens = komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
    tokens
  }

  def arrayNouns(sentence: String) = {

  }
}

object HangleTokenizer {
  def apply() : HangleTokenizer = new HangleTokenizer
}


object CrawledProcessing extends SparkStreamingInit {

  def main(args: Array[String]): Unit = {
    println("Active System ..")
//    val db : Database = Database.forURL(
//      url ="jdbc:mysql://192.168.35.123:3306/horus?useSSL=false", //?enabledTLSProtocols=TLSv1.2
//      user="root",
//      password="18651865",
//      driver = "com.mysql.jdbc.Driver")

    val anchors = ssc.receiverStream(new MySqlSourceReceiver)
    val words = anchors.flatMap(anchor => {
      HangleTokenizer().arrayTokens(anchor)
    })

//    val words = anchors.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print
//    wordCounts.foreachRDD((a, b) => {println(a + "->" + b)})
    wordCounts.foreachRDD(rdd => {
      rdd.foreach(tf => {
//        println(tf._1 + " --> " + tf._2)
        InfluxClient.writeTf(1L, tf._1, tf._2)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
