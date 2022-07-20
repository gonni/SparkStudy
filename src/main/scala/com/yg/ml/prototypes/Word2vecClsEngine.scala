package com.yg.ml.prototypes

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, stddev, udf, variance}

object Word2vecClsEngine {

  val euclideanDistance = udf { (v1: Vector, v2: Vector) =>
    math.sqrt(Vectors.sqdist(v1, v2))
  }


  val revertDouble: UserDefinedFunction = udf((v: Double) => 1 - v)

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val conf = new SparkConf()
      .setAppName("Mysql Selection")
      .setMaster("local")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val model = Word2VecModel.load("data/w2vNews2Cont_100_8")
    println("->" + math.pow(3, 2))

    calcScores("경제", 1000, model, spark)




    //    println("---------------------------------------")
    //    val synonyms = model.findSynonyms("증시", 1000)
    //    synonyms.show()
    //
    //    println("---------------------------------------")
    //    synonyms.select(variance($"similarity"), stddev($"similarity")).show()
    //
    //
    //
    //
    //        val vector1 = model.getVectors.filter($"word" === "대통령").first().getAs[Vector](1)
    ////    println("Vector -> " + vector1)
    ////
    ////    val vector3 = model.getVectors.filter($"word" === "날씨").first().getAs[Vector](1)
    ////    println("대통령 : 날씨 => " + math.sqrt(Vectors.sqdist(vector1, vector3)))
    ////
    //
    //    model.findSynonymsArray("대통령", 100).foreach(t => {
    //      var vector2 = model.getVectors.filter($"word" === t._1).first().getAs[Vector](1)
    //
    //      println(s"대통령 : ${t._1} -- cosine --> ${t._2} : == distance ==>" +
    //        s" ${math.sqrt(Vectors.sqdist(vector1, vector2))}")
    //    })

  }

  def calcScores(word: String, topN: Int, model: Word2VecModel, spark: SparkSession): Unit = {
    import spark.implicits._

    val topNcosDf = model.findSynonyms(word, topN)
    val exNcosDf = topNcosDf.withColumn("rvsim", revertDouble($"similarity"))
    exNcosDf.show()

    // 분산을 구하자
    //    +------------+------------------+-------------------+
    //    |        word|        similarity|              rvsim|
    //    +------------+------------------+-------------------+
    //    |        위기|0.6967992782592773|0.30320072174072266|
    //    |        극복| 0.614321768283844|  0.385678231716156|

    val vAvg = exNcosDf.select(avg($"rvsim")).first().getAs[Double](0).toDouble
    println("AVG => " + vAvg)
    //     exNcosDf.select(avg($"similarity")).show()
    val vPosSum = exNcosDf.map(r => {
      var vval = r.getAs[Double]("rvsim").toDouble
      math.pow(vval - vAvg, 2)
    }).reduce((a, b) => a + b).toDouble

    // 분산 함수에 의한 분산/표준편차 값
    exNcosDf.select(variance($"rvsim"), stddev($"rvsim")).show()
    println("VOONSAN => " + (vPosSum / topN))

    //    val v = exNcosDf.select(variance($"rvsim"), stddev($"rvsim"))
    //      .first().getAs[Double](0).toDouble
    val v = vPosSum / topN
    println("Variance -> " + v)

    //    +------------+------------------+-------------------+
    //    |        word|        similarity|              rvsim|
    //    +------------+------------------+-------------------+

    var idx = 1
    exNcosDf.foreach(row => {
      //      var simDistVal = 1 - row.getAs[Double](1).toFloat
      var distPow = math.pow(row.getAs[Double](2).toFloat, 2)

      var weight = math.exp(-1 * distPow / (10 * math.sqrt(v)))
      println(idx + "\t" + row.mkString(", ") + "==> w:" + weight)
      idx += 1
    })


  }
}
