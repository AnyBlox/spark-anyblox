import Dependencies._

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "org.anyblox"
ThisBuild / organizationName := "anyblox"

val sparkVersion = "3.5.0"
val parquetVersion = "1.13.1" // Same version as Parquet
val arrowVersion = "17.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "anyblox-spark",
    libraryDependencies += munit % Test,
    libraryDependencies += "org.scala-lang" %% "toolkit" % "0.1.7",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.parquet" % "parquet-column" % parquetVersion,
      "org.apache.arrow" % "arrow-c-data" % arrowVersion,
      "org.apache.arrow" % "arrow-format" % arrowVersion,
      "org.apache.arrow" % "arrow-memory-core" % arrowVersion,
      "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
      "org.apache.arrow" % "arrow-vector" % arrowVersion))

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
