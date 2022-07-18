package com.yg.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.SparkSession

object LdaClassifier {
  def main(args: Array[String]): Unit = {
    // init spark core
    val conf = new SparkConf()
      .setAppName("LDA")
      .setMaster("local")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("/Users/a1000074/dev/works/SparkStudy/data/sample_lda_data.txt")

    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
  }
}
