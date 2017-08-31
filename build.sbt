import scala.collection.Seq

name := "content_reco"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0" % "provided",
  "joda-time" % "joda-time" % "2.7",
  "org.joda" % "joda-convert" % "1.2",
  "org.apache.commons" % "commons-lang3" % "3.3.2",
  "org.apache.spark" % "spark-mllib_2.10" % "2.1.0" % "provided",
  "com.amazonaws" %   "aws-java-sdk" % "1.3.11",
  "postgresql" % "postgresql" % "9.1-901.jdbc4",
  "com.github.fommil.netlib" % "all" % "1.1.2",
  "org.scala-lang.modules" %% "scala-pickling" % "0.10.1",
  "com.jcraft" % "jsch" % "0.1.50",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0")
