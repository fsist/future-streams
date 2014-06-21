name := "foresight-bytestreams"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings")

version := "0.1"

organization := "com.fsist"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.3", // For the ByteString type
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test"
)

