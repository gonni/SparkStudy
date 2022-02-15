package com.yg
import org.apache.spark.ml.feature.StopWordsRemover

object StopwordConsole {
  def main(args: Array[String]): Unit = {
    val spark = SparkCore.createNewSpark("my")

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)
  }
}
