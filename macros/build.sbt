name := "future-streams-macros"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings")

version := "0.1"

organization := "com.fsist"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "org.scalamacros" %% "quasiquotes" % "2.0.0" % "compile",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "org.scala-lang.modules" %% "scala-async" % "0.9.1",
  "org.scalatest" %% "scalatest" % "2.1.5" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test"
)

