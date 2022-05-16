name := "SparkStudy"

version := "0.1"

ThisBuild / scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"
val akkaVersion = "2.5.26"
val akkaHttpVersion = "10.1.11"

resolvers += "jitpack" at "https://jitpack.io"


lazy val root = (project in file("."))
  .settings(
    name := "sparkGrd",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "mysql" % "mysql-connector-java" % "5.1.44",
      "com.github.shin285" % "KOMORAN" % "3.3.4",
      //"com.typesafe.akka" %% "akka-actor" % akkaVersion,
      //"com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      // akka streams
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      //"com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
        // akka http
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.influxdb" % "influxdb-client-scala_2.12" % "6.0.0"
    )
  )
