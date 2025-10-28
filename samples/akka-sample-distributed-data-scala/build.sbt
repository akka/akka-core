import com.typesafe.sbt.MultiJvmPlugin.multiJvmSettings

val AkkaVersion = "2.10.11"
val AkkaDiagnosticsVersion = "2.1.1"
val LogbackClassicVersion = "1.5.18"
val ScalaTestVersion = "3.2.17"

val `akka-sample-distributed-data-scala` = project
  .in(file("."))
  .settings(multiJvmSettings: _*)
  .settings(
    organization := "com.lightbend.akka.samples",
    version := "1.0",
    scalaVersion := "2.13.12",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    run / javaOptions ++= Seq("-Xms128m", "-Xmx1024m"),
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion,
        "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
        "com.lightbend.akka" %% "akka-diagnostics" % AkkaDiagnosticsVersion,
        "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion % Test,
        "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
        "ch.qos.logback" % "logback-classic" % LogbackClassicVersion % Test,
        "org.scalatest" %% "scalatest" % ScalaTestVersion % Test),
    run / fork := true,
    // disable parallel tests
    Test / parallelExecution := false,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument("-oDF"),
    Test / logBuffered := false,
    licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0"))))
  .configs(MultiJvm)
