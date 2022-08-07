package com.yg.tsdb

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.influxdb.client.scala.InfluxDBClientScalaFactory

import com.influxdb.client.{InfluxDBClientFactory, WriteApi, WriteOptions}
import com.influxdb.client.write.Point

import com.influxdb.query.FluxRecord

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration.Duration


object Influx22Sample {
  implicit val system: ActorSystem = ActorSystem("it-tests")

  def main(args: Array[String]): Unit = {
    println("Active System at " + new File(".").getAbsolutePath)

    writeSomething

    readSomething
  }

  def writeSomething: Unit = {
    // You can generate a Token from the "Tokens Tab" in the UI
    val token = "5nWBmnhyUFbfF3q3F_yAfr4Wklis0HQT0UFKU2qf3z29bbsGMjPxYBeP34oz__byN8aSmS4hYud2zlR8tewDrA=="
    val org = "NA"
    val bucket = "hell"

    val client = InfluxDBClientScalaFactory.create("http://localhost:8086", token.toCharArray, org)

//    /*val writeApi: WriteApi = client.makeWriteApi(WriteOptions.builder().flushInterval(5000).build())
//    val point = Point
//      .measurement("HellDb04")
//      .addTag("host", "server04")
//      .addTag("subKey", "abcd1234#")
//      .addField("value", (Math.random() * 100).intValue())
//      .addField("val2", (Math.random() * 100).intValue())
//
//    println(s"DataPoint to be added : ${point.toLineProtocol}")
//    writeApi.writePoint(point)
//
//    client.close()
//    system.terminate()*/
  }

  def readSomething: Unit = {
    //    println("Active System at " + new File(".").getAbsolutePath)

    //    val myToken = "CwgQWYIZKOcSpdlxwpfZfvDWQXpsfTlt7o2GD5hFAs4rTvHDF-7cfwmIQnmdocqL__5uoabCFGuf_GYzFQfxIA==";
    //    val myOrg = "xwaves"

    val influxDBClient = InfluxDBClientScalaFactory.create()
    //      .create("http://localhost:8086", myToken.toCharArray, myOrg)

    val fluxQuery = ("from(bucket: \"mydb\")\n"
      + " |> range(start: -1d)"
      + " |> filter(fn: (r) => (r[\"_measurement\"] == \"HellDb04\"))")

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
