package com.yg.ml.classifier

import com.yg.ml.prototypes.Word2vecSample.komoran
import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL
import kr.co.shineware.nlp.komoran.core.Komoran
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, lit, stddev, typedLit, udf, variance}

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

  val model = Word2VecModel.load("data/w2vNews2Cont_v200_m8_w7_it8")
  val topics = Seq("경제", "사건", "대통령", "주식", "화폐", "사건",
    "날씨", "북한", "이재명", "금리", "연봉", "코로나", "러시아", "IT",
    "중국", "미국", "수소", "원유", "휘발유", "디젤", "물가", "부동산",
    "에너지", "공포", "전쟁")

  val revertDouble: UserDefinedFunction = udf((v: Double) => 1 - v)

  def init() = {
    val ts = System.currentTimeMillis
    topics.distinct.foreach(topic => saveCosData(topic, ts))
  }

  // distance 값 추출
  val getTermDistance = udf{ (rvsim: Double, vari: Double) =>
    val distPow = math.pow(rvsim, 2)
    math.exp(-1 * distPow / (10 * math.sqrt(vari)))
  }

  private[this] def saveCosData(topicWord: String, ts: Long) = {
    import spark.implicits._

    val res = model.findSynonyms(topicWord, 200)
    val exRes = res.withColumn("rvsim", revertDouble($"similarity"))
      .withColumn("base_term", typedLit(topicWord))
    val v : Double = exRes.select(variance($"rvsim")).first().getAs[Double](0).toDouble

    val allDf = exRes.withColumn("dist", getTermDistance($"rvsim", lit(v)))

//    allDf show

    val dbAll = allDf.withColumnRenamed("base_term", "BASE_TERM")
      .withColumnRenamed("word", "COMP_TERM")
      .withColumnRenamed("dist", "DIST_VAL")
      .withColumn("GRP_TS", typedLit(ts))

    val dbAll2 = dbAll.select("BASE_TERM", "COMP_TERM", "DIST_VAL", "GRP_TS")

    dbAll2 show

    println("Write Data to DB.Table ------------------------")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "18651865")

    dbAll2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/horus?" +
          "useUnicode=true&characterEncoding=utf8&useSSL=false",
          "TERM_DIST", prop)
  }

  def main(args: Array[String]): Unit = {
    println("Run Topic Classifier based on Word2vec ..")
//    saveCosData("코로나")
    init
  }

}
