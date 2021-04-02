import Dependencies._

organization := "com.github.meln1k"

name := "akka-persistence-foundationdb"

version := "0.1.1"

scalaVersion := "2.13.5"

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

parallelExecution in Test := false

fork in Test := true

scalafmtOnCompile in ThisBuild := true

libraryDependencies ++= akkaPersistenceFoundationDependencies

licenses += ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))
