package com.yg.ml.prototypes

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.jdk.CollectionConverters.asScalaBufferConverter

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

    //    model.getVectors.filter($"word" === "사건").map(row => {
    //      row.getAs[Vector](1).toArray
    //    }).first().map(_.toFloat).foreach(println)

  }

  def getCosSimilarity(base: String, target: String, model: Word2VecModel, spark: SparkSession): Float = {
//    import spark.implicits._
//    val vectorSize = model.getVectorSize
//    val compFvector = model.getVectors.filter($"word" === target)
//      .map(_.getAs[Vector](1).toArray).first().map(_.toFloat)
//
//    val vecNormComp = blas.snrm2(vectorSize, compFvector, 1)
//    println("COMP BLAS =>" + vecNormComp)
//    // ----
//
//    val baseFvector = model.getVectors.filter($"word" === base)
//      .map(_.getAs[Vector](1).toArray).first().map(_.toFloat)
//
//    val vecNormBase = blas.snrm2(vectorSize, baseFvector, 1)
//    println("BLAS => " + vecNormBase)
//
//    if (vecNormBase != 0.0f) {
//      blas.sscal(vectorSize, 1 / vecNormBase, baseFvector, 0, 1)
//    }
//    //    blas.sgemv(
//    //      "T", vectorSize, 1, 1.0f,
//    //    )
//
//    if (vecNormBase == 0.0f || vecNormComp == 0.0f)
//      0.0f
//    else {
//      var res: Float = 0.0f;
//      res = 1.0f / vecNormComp
//      res
//    }
    ???
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    println("Active System ..")
    //    loadModelSample
    val model = Word2VecModel.load("data/w2vNews2Cont_100_8")

    println("---------------------------------------")
    var synonyms = model.findSynonyms("스포츠", 30)
    synonyms.show()
    println("---------------------------------------")




    //    val documentDF = spark.createDataFrame(Seq(
    //      "Hi I heard about Spark".split(" "),
    //      "I wish Java could use case classes".split(" "),
    //      "Logistic regression models are neat".split(" ")
    //    ).map(Tuple1.apply)).toDF("text")
    //
    //    documentDF.show()

    //    model.getVectors.show(300)
    //    println("cosine similarity value = " + getCosSimilarity("사건", "사고", model, spark))


    //    val model = Word2VecModel.load("data/w2vNews2Cont_200_8")
    //    model.getVectors.show(300)
    //
    //    println("---------------------------------------")
    //    var synonyms = model.findSynonyms("경제", 30)
    //    synonyms.show()
    //    println("---------------------------------------")
    //
    //    synonyms = model.findSynonyms("사회", 30)
    //    synonyms.show()
    //    println("---------------------------------------")
    //
    //    synonyms = model.findSynonyms("정치", 30)
    //    synonyms.show()
    //    println("---------------------------------------")
    //
    //    synonyms = model.findSynonyms("주식", 30)
    //    synonyms.show()
    //
    //    println("*---------------------------------------")
    //
    //    model.findSynonymsArray("반려", 10)
    //    model.getVectors.filter($"word" === "사건").show()
    //    println("*---------------------------------------")
    //
    //    model.getVectors.filter($"word" === "사건").map(row => {
    //      row.getAs[Vector](1).toArray
    //    }).first().map(_.toFloat).foreach(println)


    //    val documentDF = spark.createDataFrame(Seq(
    //      "Hi I heard about Spark".split(" "),
    //      "I wish Java could use case classes".split(" "),
    //      "Logistic regression models are neat".split(" ")
    //    ).map(Tuple1.apply)).toDF("tokenized")
    //    documentDF.show()

    //    val prop = new Properties()
    //    prop.put("user", "root")
    //    prop.put("password", "18651865")
    //
    //    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/horus?" +
    //      "useUnicode=true&characterEncoding=utf8&useSSL=false",
    //      "crawl_unit1", prop)
    //
    //    println("Data from mysql with new column ..")
    //    //    tableDf.withColumn("aa", $"ANCHOR_TEXT").show(15)
    //    val tokenizedData = tableDf.filter($"SEED_NO" === 9)
    //      .orderBy(desc("CRAWL_NO"))
    //      .select($"ANCHOR_TEXT",$"PAGE_TEXT")
    //      .withColumn("tokenized", getTokenListUdf2($"ANCHOR_TEXT"))
    //
    //    println("*---------------------------------------")
    //    val result = model.transform(tokenizedData).select($"tokenized", $"vector")
    //    result.show()
    //
    //    println("########################################")


    //    model.getVectors.filter($"word" === "수영장").g

    //    println("########################################")
    //    result.collect().foreach {
    //      case Row(text: Seq[_], features: Vector) =>
    //        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    //    }
    //    val data =spark.createDataFrame(Seq("대통령", "정치").map(Tuple1.apply)).toDF("tokenized")
    //    model.transform(data).show()

    //    val vectors = model.getVectors
    //    val vec = vectors.filter($"word" === "정치").first()
    //    val res = vec.getAs[Vector]("vector")
    //    res.
  }


}
