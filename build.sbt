name := "fsist-streams"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings")

version := "0.1"

organization := "com.fsist"

retrieveManaged := true // Put dependency source and doc jars in lib_managed, not just user homedir

lazy val streams = project

lazy val bytestreams = project.dependsOn(streams % "compile->compile;test->test")

