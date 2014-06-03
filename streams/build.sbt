name := "fsist-streams"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings")

version := "0.1"

organization := "com.fsist"

libraryDependencies ++= Seq(
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "org.scala-lang.modules" %% "scala-async" % "0.9.1",
  "org.reactivestreams" % "reactive-streams-spi" % "0.3",
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test"
)

