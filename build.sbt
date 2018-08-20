import Dependencies._

organization := "com.github.meln1k"

name := "akka-persistence-foundationdb"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.6"

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  //"-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

parallelExecution in Test := false

scalafmtOnCompile in ThisBuild := true

libraryDependencies ++= akkaPersistenceFoundationDependencies
