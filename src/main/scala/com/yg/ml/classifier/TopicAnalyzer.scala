package com.yg.ml.classifier

import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.SparkSession

object TopicAnalyzer {
  case class TopicScore(topic: String, score: Double)

  val spark = SparkSession.builder()
    .appName("W2vTopicCls")
    .master("local")
    .getOrCreate()

  val model = Word2VecModel.load("data/w2vNews2Cont_100_8")

  def analyzeTopic(sentence : String) : Map[String, Double] = {
    model.findSynonyms("대통령", 10).show()
    ???
  }

  def init() = {

  }

  def main(args: Array[String]): Unit = {
    println("Run Topic Classifier based on Word2vec ..")
    analyzeTopic("가나다라 테스트 바하무트 ..")

  }
}
