import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

val AkkaVersion = "2.10.14"
val AkkaDiagnosticsVersion = "2.1.0"
val LogbackClassicVersion = "1.2.11" 
val ScalaTestVersion = "3.1.1"

val `akka-sample-distributed-data-java` = project
  .in(file("."))
  .settings(multiJvmSettings: _*)
  .settings(
    organization := "com.lightbend.akka.samples",
    version := "1.0",
    scalaVersion := "2.13.12",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    Compile / javacOptions ++= Seq("-parameters", "-Xlint:unchecked", "-Xlint:deprecation", "-Xdiags:verbose"),
    javaOptions in run ++= Seq("-Xms128m", "-Xmx1024m"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
      "com.lightbend.akka" %% "akka-diagnostics" % AkkaDiagnosticsVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
      "ch.qos.logback" % "logback-classic" % LogbackClassicVersion % Test,
      "org.scalatest" %% "scalatest" % ScalaTestVersion % Test),
    fork in run := true,
    // disable parallel tests
    parallelExecution in Test := false,
    // show full stack traces and test case durations
    testOptions in Test += Tests.Argument("-oDF"),
    logBuffered in Test := false,
    licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0")))
  )
  .configs (MultiJvm)
