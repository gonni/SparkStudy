package com.yg.influx

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer

import java.net.URLEncoder
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object InfluxHttpClient {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  def writeData(rawData: String): String = {
    val request = HttpRequest(
      method = HttpMethods.POST,
//      uri = RuntimeConfig().getString("cpa_admin.cpa_api_write_url"),
      uri = "http://localhost:8086/write?db=mydb",
      entity = HttpEntity(
        rawData
      )
    )

    val res = Await.result(Http().singleRequest(request), 10.seconds)
    res.status.value
  }

  def main(args: Array[String]): Unit = {
    println("Active System ..")
    writeData("cpu_load_short,host=server09,region=ca-west value=0.64 1434055562000000000")
    println("Closed ..")

//    implicit val system = ActorSystem(Behaviors.empty, "SingleRequest")
//    // needed for the future flatMap/onComplete in the end
//    implicit val executionContext = system.executionContext
//
//    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = "http://akka.io"))
//
//    responseFuture
//      .onComplete {
//        case Success(res) => println(res)
//        case Failure(_)   => sys.error("something wrong")
//      }
  }
}
