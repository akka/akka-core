organization := "com.lightbend.akka.samples"
name := "akka-sample-fsm-scala"

val AkkaVersion = "2.10.16"
val LogbackClassicVersion = "1.5.1"
val AkkaDiagnosticsVersion = "2.1.0"

scalaVersion := "2.13.17"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "ch.qos.logback" % "logback-classic" % LogbackClassicVersion,
  "com.lightbend.akka" %% "akka-diagnostics" % AkkaDiagnosticsVersion
)

run / fork := true

licenses := Seq(
  ("CC0", url("http://creativecommons.org/publicdomain/zero/1.0"))
)
