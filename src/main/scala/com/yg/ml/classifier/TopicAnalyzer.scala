package com.yg.ml.classifier

import com.yg.ml.prototypes.Word2vecSample.komoran
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{avg, stddev, udf, variance}

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.reflect.internal.util.TableDef.Column

// 각 토픽별로 Score를 산출하는 모듈 - 경제, 사건, 대통령, 주식, 화폐, 코인, 날씨, 북한, 이재명, 금리, 연봉, 코로나, 손흥민
object TopicAnalyzer {
  case class TopicScore(topic: String, score: Double)

  val spark = SparkSession.builder()
    .appName("W2vTopicCls")
    .master("local")
    .getOrCreate()

  val model = Word2VecModel.load("data/w2vNews2Cont_100_8")
  val topics = Seq("경제", "사건", "대통령", "주식", "화폐", "화폐", "날씨", "북한", "이재명", "금리", "연봉", "코로나")
  val revertDouble: UserDefinedFunction = udf((v: Double) => 1 - v)

  var distMatrix = mutable.HashMap[String, mutable.Map[String, Double]]()

  val komoran = new Komoran(DEFAULT_MODEL.LIGHT)

  def analyzeTopic(sentence : String) : mutable.HashMap[String, Double] = {
    val result = mutable.HashMap[String, Double]()
    val tokens = komoran.analyze(sentence).getTokenList.asScala
//    komoran.analyze(sentence).getTokenList.asScala.map(_.getMorph)
    tokens.foreach(token => {
//      println(token.getMorph)
      for (topic <- topics) {
        var termScore = this.distMatrix.get(topic).map{kv => {
            kv.get(token.getMorph).getOrElse[Double](0)
          }
        }.foldLeft(0: Double)(_.toDouble + _.toDouble)
        result.put(topic, termScore)
      }
    })
    result
  }

  def getTpicScore(sentence: String, topic: String) = {
    val topicTerms = this.distMatrix.get(topic)

    val tokens = komoran.analyze(sentence).getTokenList.asScala
    tokens.map(token => {
      topicTerms.getOrElse(token, 0)
    }).foreach(println)

  }

  def init() = {
//    val topics = Seq("경제", "사건", "대통령", "주식", "화폐", "화폐", "날씨", "북한", "이재명", "금리", "연봉", "코로나", "외계")
    topics.foreach(initCosData)

    println("Initialized Topic Size : " + distMatrix.size)
    distMatrix.foreach{case(k,v) =>
      {
        println(k)
        for (elem <- v) {
          println(elem)
        }
        println("-----------------")
      }
    }
  }

  private[this] def initCosData(topicWord: String) = {
    import spark.implicits._


    val res = model.findSynonyms(topicWord, 5)
    val exRes = res.withColumn("rvsim", revertDouble($"similarity"))
    exRes show

    val vAvg = exRes.select(avg($"rvsim")).first().getAs[Double](0).toDouble
    println("AVG => " + vAvg)

    val v = exRes.select(variance($"rvsim")).first().getAs[Double](0).toDouble
    println("Variance => " + v)

    var idx = 1
    val mapDist1 = mutable.Map[String, Double]()

//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "18651865")

    for(row <- exRes) {
      var distPow = math.pow(row.getAs[Double](2).toFloat, 2)
      var weight = math.exp(-1 * distPow / (10 * math.sqrt(v)))
//      println(idx + "\t" + row.mkString(", ") + "==> w:" + weight)
      mapDist1 update (row.getAs[String](0).toString , weight)
    }

    exRes.withColumn("BASE_WORD", Column.apply[String])


//    this.distMatrix.put(topicWord, mapDist1)
  }



  def main(args: Array[String]): Unit = {
    println("Run Topic Classifier based on Word2vec ..")
//    analyzeTopic("가나다라 테스트 바하무트 ..")
    initCosData("코로나")
//    init()
//    getTpicScore("[속보] 日, 신규 확진 20만명 돌파…17개 도부현에서 역대 최다 경신", "코로나")
  }
}
