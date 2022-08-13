package com.yg.ml.kor

import com.yg.ml.prototypes.Word2vecSample.{getTokenListUdf2, main}
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, countDistinct, desc, explode, length, size, udf}

import java.util.Properties
import scala.jdk.CollectionConverters.asScalaBufferConverter

trait SparkStreamingInit extends Serializable {
//  val conf = new SparkConf().setMaster("local[8]").setAppName("SparkTemplate")
//  val ssc = new StreamingContext(conf, Seconds(10))
  val conf = new SparkConf()
  .setAppName("Mysql Selection")
  .setMaster("local[8]")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  def tfidfOnSparkSample(): Unit = {
    import spark.implicits._

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()
    println("------")
    rescaledData.show()
  }
}

class TfidfOnSparkImpl extends SparkStreamingInit with Serializable {

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    if(sentence != null) {
      val komoran = new Komoran(DEFAULT_MODEL.LIGHT)
      komoran.analyze(sentence).getNouns.asScala
    } else {
      Seq[String]()
    }
  }

  val calcIdfUdf1 = udf { df: Long => TfidfOnSpark.calcIdf(100L, df) }

  def tfidf: Unit = {
    import spark.implicits._
    println("Active System ..")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/horus?" +
      "useUnicode=true&characterEncoding=utf8&useSSL=false",
      "crawl_unit1", prop)

    val tokenizedData = tableDf.filter($"SEED_NO" === 3)
      .orderBy(desc("CRAWL_NO"))
      .select($"CRAWL_NO", $"ANCHOR_TEXT", $"PAGE_TEXT")
      .withColumn("document", getNounsUdf($"PAGE_TEXT"))
      .withColumn("token_size", size(col("document"))).limit(100)

//    tokenizedData.show()

    val documents = tokenizedData.select($"document", $"CRAWL_NO" as "doc_id")
    documents.show


    val columns = documents.columns.map(col) :+ (explode(col("document")) as "token")
    val unfoldedDocs = documents.select(columns: _*)
//    unfoldedDocs.show

    val tokenWithTf = unfoldedDocs.groupBy("doc_id", "token").agg(count("document") as "tf")

    val tokenWithDf = unfoldedDocs.groupBy("token").agg(countDistinct("doc_id") as "df")
    println("=>" + documents.count())
    val tokenWithIdf = tokenWithDf.withColumn("idf", calcIdfUdf1(col("df")))


    val tfidf = tokenWithTf.join(tokenWithIdf, Seq("token"), "left").withColumn("tf_id", col("tf") * col("idf"))

    tfidf show 300

    println("Finished")

//    tokenizedData.where($"token_size" > 0).show()

//    tokenizedData.filter(size(col("tokenized")) > 0).show()


  }
}

object TfidfOnSpark {
  def calcIdf(docCount: Long, df: Long): Double =
    math.log((docCount.toDouble + 1) / (df.toDouble + 1))

  def main(args: Array[String]): Unit = {
    val test = new TfidfOnSparkImpl
    test.tfidf
  }
}
