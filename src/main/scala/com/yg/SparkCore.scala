package com.yg

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkCore {
  def createNewSpark(name: String) = {
    SparkSession.builder()
      .appName(name)
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }
}
