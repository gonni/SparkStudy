package com.yg.tsdb

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.influxdb.client.scala.InfluxDBClientScalaFactory
import com.influxdb.query.FluxRecord

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration.Duration


object Influx22Sample {
  implicit val system: ActorSystem = ActorSystem("it-tests")

  def main(args: Array[String]): Unit = {
//    println("Active System at " + new File(".").getAbsolutePath)

//    val myToken = "CwgQWYIZKOcSpdlxwpfZfvDWQXpsfTlt7o2GD5hFAs4rTvHDF-7cfwmIQnmdocqL__5uoabCFGuf_GYzFQfxIA==";
//    val myOrg = "xwaves"

    val influxDBClient = InfluxDBClientScalaFactory.create()
//      .create("http://localhost:8086", myToken.toCharArray, myOrg)

    val fluxQuery = ("from(bucket: \"mydb\")\n"
      + " |> range(start: -1d)"
      + " |> filter(fn: (r) => (r[\"_measurement\"] == \"HellDb\" and r[\"host\"] == \"myHost\"))")

    //Result is returned as a stream
    val results = influxDBClient.getQueryScalaApi().query(fluxQuery)

    //Example of additional result stream processing on client side
    val sink = results
      //filter on client side using `filter` built-in operator
      //.filter(it => "cpu0" == it.getValueByKey("cpu"))

      //take first 20 records
      .take(20)
      //print results
      .runWith(Sink.foreach[FluxRecord](it => println(s"Measurement: ${it.getMeasurement}, value: ${it.getValue}")
      ))

    // wait to finish
    Await.result(sink, Duration.Inf)

    influxDBClient.close()
    system.terminate()
  }
}
