package com.yg.tsdb

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer

import java.net.URLEncoder
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Influx18Sample {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  val source =
    """
      |object SimpleApp {
      |  val aField = 2
      |
      |  def aMethod(x: Int) = x + 1
      |
      |  def main(args: Array[String]) = {
      |    println(aMethod(aField))
      |  }
      |}
    """.stripMargin

  val request = HttpRequest(
    method = HttpMethods.POST,
    uri = "http://markup.su/api/highlighter",
    entity = HttpEntity(
      ContentTypes.`application/x-www-form-urlencoded`,
      s"source=${URLEncoder.encode(source.trim, "UTF-8")}&language=Scala&theme=Sunburst"
    )
  )

  def sendRequest() = {
    val responseFuture: Future[HttpResponse] = Http().singleRequest(request)
    responseFuture.onComplete{
      case Success(res) => {
          val entityFuture: Future[HttpEntity.Strict] = responseFuture.flatMap(_.entity.toStrict(2 seconds))
          entityFuture.map(entity => entity.data.utf8String)
      }
      case Failure(_) => println("Error Response")
    }
    responseFuture.andThen{
      case _ => {
        println("Terminate system ..")
        system.terminate()
      }
    }
  }

//  def simpleRequest() = {
//    val responseFuture = Http().singleRequest(request)
//    responseFuture.flatMap(_.entity.toStrict(2 seconds)).map(_.data.utf8String).foreach(println)
//  }

  def main(args: Array[String]): Unit = {
    println("Active")
    sendRequest()
//    sendRequest().foreach(println)
  }
}
