package com.yg.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import java.util.Properties
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.apache.spark.ml.linalg.Vector

object Word2vecClass {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

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
    def loadModelSample = {
    val model = Word2VecModel.load("data/w2vNews2Cont_200_8")
    model.getVectors.show(300)
    println("---------------------------------------")
    var synonyms = model.findSynonyms("경제", 30)
    synonyms.show()
    println("---------------------------------------")

    synonyms = model.findSynonyms("사회", 30)
    synonyms.show()
    println("---------------------------------------")

    synonyms = model.findSynonyms("정치", 30)
    synonyms.show()
    println("---------------------------------------")

    synonyms = model.findSynonyms("주식", 30)
    synonyms.show()



  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("Active System ..")

    //    loadModelSample
    val model = Word2VecModel.load("data/w2vNews2Cont_200_8")
    model.getVectors.show(300)
    println("---------------------------------------")
    var synonyms = model.findSynonyms("경제", 30)
    synonyms.show()
    println("---------------------------------------")

    synonyms = model.findSynonyms("사회", 30)
    synonyms.show()
    println("---------------------------------------")

    synonyms = model.findSynonyms("정치", 30)
    synonyms.show()
    println("---------------------------------------")

    synonyms = model.findSynonyms("주식", 30)
    synonyms.show()

    println("*---------------------------------------")
//    val documentDF = spark.createDataFrame(Seq(
//      "Hi I heard about Spark".split(" "),
//      "I wish Java could use case classes".split(" "),
//      "Logistic regression models are neat".split(" ")
//    ).map(Tuple1.apply)).toDF("tokenized")
//    documentDF.show()
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
      .select($"ANCHOR_TEXT",$"PAGE_TEXT")
      .withColumn("tokenized", getTokenListUdf2($"ANCHOR_TEXT"))

    println("*---------------------------------------")
    val result = model.transform(tokenizedData).select($"tokenized", $"vector")
    result.show()

    println("########################################")
//    model.getVectors.filter($"word" === "수영장").g

//    println("########################################")
//    result.collect().foreach {
//      case Row(text: Seq[_], features: Vector) =>
//        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
//    }
//    val data =spark.createDataFrame(Seq("대통령", "정치").map(Tuple1.apply)).toDF("tokenized")
//    model.transform(data).show()

    val vectors = model.getVectors
    val vec = vectors.filter($"word" === "정치").first()
    val res = vec.getAs[Vector]("vector")
    res.
  }
}
