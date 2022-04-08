package com.yg

import com.yg.influx.InfluxHttpClient
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, split, struct, udf}
import org.apache.spark.sql.types.{StringType, StructType}

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

object KorAnalyzer {
  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  val getPlainTextUdf: UserDefinedFunction = udf[String, String] { sentence =>
    komoran.analyze(sentence).getPlainText
  }

  val getNounsUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getNouns.asScala
  }

  val getTokenListUdf: UserDefinedFunction = udf[Seq[String], String] { sentence =>
    komoran.analyze(sentence).getTokenList.asScala.map(x => x.toString)
  }

  val getNounListStrUdf: UserDefinedFunction = udf[String, String] { sentence =>
    komoran.analyze(sentence).getNouns.asScala.mkString("|")
  }

  val func = udf((s:String) => if(s.length > 30) "length:" + s.length else s)

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("Mysql Selection")
//      .setMaster("local")
//
//    val spark = SparkSession.builder.config(conf).getOrCreate()
    val spark = SparkSession.builder()
  .appName("MySpark")
  .master("local[*]")
  .config("spark.driver.bindAddress", "127.0.0.1")
  .getOrCreate()

    import spark.implicits._

    val testDataset = spark.createDataFrame(Seq(
      "밀리언 달러 베이비랑 바람과 함께 사라지다랑 뭐가 더 재밌었어?",
      "아버지가방에들어가신다",
      "나는 밥을 먹는다",
      "하늘을 나는 자동차",
      "아이폰 기다리다 지쳐 애플공홈에서 언락폰질러버렸다 6+ 128기가실버ㅋ"
    ).map(Tuple1.apply)).toDF("sentence")

    // 1. print test data
    testDataset.show(truncate = false)

    val analyzedDataset =
      testDataset.withColumn("plain_text", getPlainTextUdf($"sentence"))
        .withColumn("nouns", getNounsUdf($"sentence"))
        .withColumn("token_list", getTokenListUdf($"sentence"))

    // 2. print test data and analyzed result as list
    analyzedDataset.select("sentence", "token_list").show()

    // 3. print test data and morphes with selected pos
    analyzedDataset.select("sentence", "nouns").show()

    // 4. print test data and analyzed result as pos-tagged text
    analyzedDataset.select("sentence", "plain_text").show()

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/horus?" +
      "useUnicode=true&characterEncoding=utf8&useSSL=false",
      "crawl_unit1", prop)

    println("Data from mysql with new column ..")
//    tableDf.withColumn("aa", $"ANCHOR_TEXT").show(15)
//    val exTbl = tableDf.withColumn("TOKENS", getTokenListUdf($"ANCHOR_TEXT"))
//    exTbl.show(10)
//
//    exTbl.select("TOKENS").map(row => {
//      row.get(0).asInstanceOf[Seq[String]].map(token => token.toString)
//    }).show(10)

    // words count
    val nouns = tableDf.withColumn("NOUNS", getNounListStrUdf($"ANCHOR_TEXT"))
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val words = nouns.select(explode(split(col("NOUNS"), "\\|")).as("word"))
    words.show(20)
    println("word count ==============")
    val countResult = words.groupBy("word").count()

    // --------------
//    countResult.sort(desc("count")).show(100)
//    val schema = StructType(Seq(
//      StructField("word", StringType),
//      StructField("count", IntegerType)
//    ))
//    val encoder = RowEncoder(schema)
//    countResult.rdd.map(row => {
//      println(s"sorted -> ${row.getString(0)} : ${row.getLong(1)}")
//    })

    countResult.foreach(row => {
//      println(s"sorted -> ${row.getString(0)} : ${row.getLong(1)}")
      val a = InfluxHttpClient.writeData(s"rt_word_count,host=localhost,word=${row.getString(0)} value=${row.getLong(1)}")
    })
    // --------------


    println("-------------------------")
    countResult.printSchema()

//    val temp = countResult
//      .withColumn("FREQ", $"count")
//      .withColumn("WORD", $"word")
//
//    println("-------------------------")
//    val finalResult = temp.select("WORD", "FREQ")
//    finalResult.show(10)


//    val finalResult = countResult
//      .withColumnRenamed("word", "WORD")
//      .withColumnRenamed("count", "FREQ")
//
//    // save TF-data to DB
//    finalResult.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/horus?" +
//      "useUnicode=true&characterEncoding=utf8&useSSL=false",
//      "NLP_TF2", prop)
////
//    println("Writing to DB completed ..")



//    println("with udf column ..")
//    tableDf.select($"CRAWL_NO", $"ANCHOR_TEXT", func($"ANCHOR_TEXT") as "XX").show(20)
//
//    tableDf.show(15)
//    tableDf.createOrReplaceTempView("tempTable")
//    tableDf.show(10)
//
//    println("Changed Query ..")
//    tableDf.createOrReplaceTempView("aya")
//    tableDf.sqlContext.sql("select * from aya where CRAWL_NO > 1000").show(10)

    println("Successfully Finished ..")
  }
}
