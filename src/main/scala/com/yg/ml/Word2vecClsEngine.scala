package com.yg.ml

import math._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.udf

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object Word2vecClsEngine {

  val euclideanDistance = udf { (v1: Vector, v2: Vector) =>
    math.sqrt(Vectors.sqdist(v1, v2))
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")

    val conf = new SparkConf()
      .setAppName("Mysql Selection")
      .setMaster("local")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val model = Word2VecModel.load("data/w2vNews2Cont_200_8")
//    model.getVectors.show(300)

    println("---------------------------------------")
    val synonyms = model.findSynonyms("대통령", 30)
    synonyms.show()

    println("---------------------------------------")
    synonyms.select(variance($"similarity"), stddev($"similarity")).show()




    //    val vector1 = model.getVectors.filter($"word" === "대통령").first().getAs[Vector](1)
//    println("Vector -> " + vector1)
//
//    val vector3 = model.getVectors.filter($"word" === "날씨").first().getAs[Vector](1)
//    println("대통령 : 날씨 => " + math.sqrt(Vectors.sqdist(vector1, vector3)))
//
//    model.findSynonymsArray("대통령", 10).foreach(t => {
//      var vector2 = model.getVectors.filter($"word" === t._1).first().getAs[Vector](1)
//
//      println(s"대통령 : ${t._1} -- cosine --> ${t._2} : == distance ==>" +
//        s" ${math.sqrt(Vectors.sqdist(vector1, vector2))}")
//    })



  }
}
