package com.yg.ml.classifier

import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{avg, stddev, udf, variance}

// 각 토픽별로 Score를 산출하는 모듈 - 경제, 사건, 대통령, 주식, 화폐, 코인, 날씨, 북한, 이재명, 금리, 연봉, 코로나, 손흥민
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

  val revertDouble: UserDefinedFunction = udf((v: Double) => 1 - v)

  private[this] def initCosData(topicWord: String) = {
    import spark.implicits._

    val res = model.findSynonyms(topicWord, 10)
    val exRes = res.withColumn("rvsim", revertDouble($"similarity"))
    exRes show

    val vAvg = exRes.select(avg($"rvsim")).first().getAs[Double](0).toDouble
    println("AVG => " + vAvg)

    val v = exRes.select(variance($"rvsim")).first().getAs[Double](0).toDouble
    println("Variance => " + v)

    var idx = 1
    exRes.foreach(row => {
      //      var simDistVal = 1 - row.getAs[Double](1).toFloat
      var distPow = math.pow(row.getAs[Double](2).toFloat, 2)
      var weight = math.exp(-1 * distPow / (10 * math.sqrt(v)))
      println(idx + "\t" + row.mkString(", ") + "==> w:" + weight)
      idx += 1
    })

  }

  def init() = {

  }

  def main(args: Array[String]): Unit = {
    println("Run Topic Classifier based on Word2vec ..")
//    analyzeTopic("가나다라 테스트 바하무트 ..")
    initCosData("경제")
  }
}
