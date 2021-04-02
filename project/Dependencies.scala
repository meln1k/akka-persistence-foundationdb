import sbt._

object Dependencies {

  val AkkaVersion = "2.6.13"

  val akkaPersistenceFoundationDependencies = Seq(
    "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
    "com.typesafe.akka"      %% "akka-cluster-tools"                  % AkkaVersion,
    "com.typesafe.akka"      %% "akka-persistence-query"              % AkkaVersion,
    "com.google.protobuf"     % "protobuf-java"                       % "3.6.1",
    "org.foundationdb"        % "fdb-java"                            % "6.2.22",


    "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion     % "test",
    "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion     % "test",
    "ch.qos.logback"          % "logback-classic"                     % "1.2.3"         % "test",
    "org.scalatest"          %% "scalatest"                           % "3.2.5"         % "test"
  )
}
