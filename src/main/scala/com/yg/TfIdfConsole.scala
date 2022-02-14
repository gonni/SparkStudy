package com.yg

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.
{StringIndexer, Tokenizer, StopWordsRemover, Word2Vec, CountVectorizer, IDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.Pipeline

object TfIdfConsole {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MySpark")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "이거 게임 정말 좋아 정말 최고야"),
      (1.0, "이거 게임 별로임")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("token")

    val tf = new CountVectorizer()
      .setInputCol("token")
      .setOutputCol("tf")
    //   .setVocabSize(3)
    //   .setMinDF(2)

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tf-idf")
    // .setMinDocFreq(0)

    val pipe_tf = new Pipeline()
      .setStages(Array(tokenizer, tf, idf))
      .fit(sentenceData)

    val transformed = pipe_tf.transform(sentenceData)
    transformed.show()

    transformed.drop("label", "sentence").createOrReplaceTempView("tf")

  }
}
