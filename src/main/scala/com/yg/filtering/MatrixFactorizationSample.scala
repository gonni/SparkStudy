package com.yg.filtering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

object MatrixFactorizationSample {
  def main(args: Array[String]): Unit = {
//    val session : SparkSession = SparkSession
//      .builder()
//      .master("local[2]")
//      .getOrCreate()

//    .master("local[*]")
//      .config("spark.driver.bindAddress", "127.0.0.1")
    val conf = new SparkConf().setMaster("local[*]").setAppName("MF_on_Spark")
    val sc = new SparkContext(conf)

    println("The Spark Session is successfully initialized ..")
//    import session.implicits._

    // Load and parse the data
    val data = sc.textFile("data/mf_test.data")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    println("---------- input -----------")
    ratings.foreach(r => {
      println(s"read input data line => ${r.user}, ${r.product}, ${r.rating}")
    })
    println("---------- /input -----------")

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println(s"Mean Squared Error = $MSE")

    // Save and load model
    val tsmp = System.currentTimeMillis()
    model.save(sc, "output/myCollaborativeFilter_" + tsmp)
    val sameModel = MatrixFactorizationModel.load(sc, "output/myCollaborativeFilter_" + tsmp)

    println("final predict 6:2 = " + sameModel.predict(6, 2) + ", 6:4 = "
      + sameModel.predict(6,4))

//    println("final predict 6:3 = " + sameModel.predict(6, 3) + ", 6:4 = "
//      + sameModel.predict(6,4))
  }
}
