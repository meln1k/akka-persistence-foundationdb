import sbt._

object Dependencies {

  val AkkaVersion = "2.5.11"

  val akkaPersistenceFoundationDependencies = Seq(
    "org.scala-lang.modules" %% "scala-async"                         % "0.9.7",
    "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
    "com.typesafe.akka"      %% "akka-cluster-tools"                  % AkkaVersion,
    "com.typesafe.akka"      %% "akka-persistence-query"              % AkkaVersion,
    "com.google.protobuf"     % "protobuf-java"                       % "3.6.0",
    "org.foundationdb"        % "fdb-java"                            % "5.2.5",


    "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion     % "test",
    "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion     % "test",
    "ch.qos.logback"          % "logback-classic"                     % "1.2.3"         % "test",
    "org.scalatest"          %% "scalatest"                           % "3.0.4"         % "test"
  )
}
