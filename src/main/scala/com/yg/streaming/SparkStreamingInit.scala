package com.yg.streaming

import org.apache.spark._
import org.apache.spark.streaming._

trait SparkStreamingInit {
  val conf = new SparkConf().setMaster("local[3]").setAppName("SparkTemplate")
  val ssc = new StreamingContext(conf, Seconds(1))
}
