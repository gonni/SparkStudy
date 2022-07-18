package com.yg.ml

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import java.util.Properties

object Word2vecSample2 {
  val getPlainTextUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.analyze(sentence).getPlainText.split("\\s")
  }

  def main(args: Array[String]): Unit = {
//    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
//    komoran.analyze("대통령실 이전에 軍사이버사령부도 연쇄이동…안보공백 우려").getPlainText.split("\\s").foreach(println)

    val conf = new SparkConf()
      .setAppName("Mysql Selection")
      .setMaster("local")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("Active System ..")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/horus?" +
      "useUnicode=true&characterEncoding=utf8&useSSL=false",
      "crawl_unit1", prop)

    println("Data from mysql with new column ..")
    //    tableDf.withColumn("aa", $"ANCHOR_TEXT").show(15)
    val tokenizedData = tableDf.filter($"SEED_NO" === 9)
      .orderBy(desc("CRAWL_NO"))
      .select($"ANCHOR_TEXT")
      .withColumn("tokenized", getPlainTextUdf2($"ANCHOR_TEXT"))


//    tokenizedData.show()

    println("----------------------------------")

//    val word2vec = new Word2Vec().set

  }
}
