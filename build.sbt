import Dependencies._

organization := "com.github.meln1k"

name := "akka-persistence-foundationdb"

version := "0.1.0"

scalaVersion := "2.12.7"

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

fork in Test := true

scalafmtOnCompile in ThisBuild := true

libraryDependencies ++= akkaPersistenceFoundationDependencies

licenses += ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))
