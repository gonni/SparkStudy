package com.yg.ml.prototypes

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{desc, udf}

import java.util.Properties
import scala.jdk.CollectionConverters.asScalaBufferConverter

object Word2vecSample {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  val getPlainTextUdf: UserDefinedFunction = udf[String, String] { sentence =>
    komoran.analyze(sentence).getPlainText
  }

  val getPlainTextUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
    komoran.analyze(sentence).getPlainText.split("\\s")
  }

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getNouns.asScala
  }

  val getTokenListUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getTokenList.asScala.map(x => x.toString)
  }

  val getTokenListUdf2: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    try {
      komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
    } catch {
      case e: Exception => {
        println("Detected Null Pointer .. " + e.getMessage)
        Seq()
      }
    }
  }

  def createModel = {
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
      .select($"ANCHOR_TEXT", $"PAGE_TEXT")
      .withColumn("tokenized", getTokenListUdf2($"PAGE_TEXT"))

    tokenizedData.show()

    val word2Vec = new Word2Vec()
      .setInputCol("tokenized")
      .setOutputCol("vector")
      .setVectorSize(10)
      .setMinCount(8)
      .setWindowSize(7)
      .setMaxIter(8)
    //      .setMaxIter(8)
    //      .setNumPartitions(8)

    val model = word2Vec.fit(tokenizedData)
    model.save("data/w2vNews2Cont_v10_m8_w7_it8")

    val result = model.transform(tokenizedData)

    println("---------------------------")
    result.show()
    //    result.collect().foreach {case Row(text: Seq[_], features: Vector) =>
    //      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")}

    println("---------------------------")
    model.findSynonyms("대통령", 30).show()

    println("Completed ..")
  }

  def loadModelSample = {
    val model = Word2VecModel.load("data/w2vNews2Cont")
    model.getVectors.show(300)
    println("---------------------------------------")
    val synonyms = model.findSynonyms("대통령/NNG", 20)
    synonyms.show()

  }

  def main(v: Array[String]): Unit = {
    //    val conf = new SparkConf()
    //      .setAppName("Mysql Selection")
    //      .setMaster("local")
    //
    //    val spark = SparkSession.builder.config(conf).getOrCreate()
    //    import spark.implicits._
    //
    //    println("Active System ..")
    //    loadModelSample

    createModel
  }
}
