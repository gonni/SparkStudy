package com.yg
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object TfIdfConsole2 {
  def main(args: Array[String]): Unit = {
    val sc = SparkCore.createNewSpark("tfidf")

    // Load documents (one per line).
//    val documents: RDD[Seq[String]] = sc.textFile("data/mllib/kmeans_data.txt")
//      .map(_.split(" ").toSeq)
    val rddFromeFile = sc.sparkContext.textFile("/Users/a1000074/dev/works/SparkStudy/sample.txt")
//    println(rddFromeFile.getClass)
    val documents: RDD[Seq[String]] = rddFromeFile.map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)

    println("---------------------------")
    tfidfIgnore.map(v => {
      println(v)
    })

  }
}
