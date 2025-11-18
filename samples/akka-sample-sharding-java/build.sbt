
val AkkaVersion = "2.10.12"
val AkkaHttpVersion = "10.7.0"
val LogbackVersion = "1.2.11"

lazy val buildSettings = Seq(
  organization := "com.lightbend.akka.samples",
  scalaVersion := "2.13.12"
)

lazy val commonJavacOptions = Seq(
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

lazy val commonSettings = Seq(
  Compile / javacOptions ++= commonJavacOptions,
  run / javaOptions ++= Seq("-Xms128m", "-Xmx1024m"),
  run / fork := true,
  licenses := Seq(
    ("CC0", url("http://creativecommons.org/publicdomain/zero/1.0"))
  )
)

lazy val killrweather = project
  .in(file("killrweather"))
  .settings(commonSettings)
  .settings(
    mainClass in (Compile, run) := Some("sample.killrweather.KillrWeather"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
      "com.typesafe.akka" %% "akka-distributed-data" % AkkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-jackson" % AkkaHttpVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion)
  )

lazy val `killrweather-fog` = project
  .in(file("killrweather-fog"))
  .settings(commonSettings)
  .settings(
    mainClass in (Compile, run) := Some("sample.killrweather.fog.Fog"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
      "ch.qos.logback" % "logback-classic" % LogbackVersion
    )
  )

// Startup aliases for the first two seed nodes and a third, more can be started.
addCommandAlias("sharding1", "runMain sample.killrweather.KillrWeather 2551")
addCommandAlias("sharding2", "runMain sample.killrweather.KillrWeather 2552")
addCommandAlias("sharding3", "runMain sample.killrweather.KillrWeather 0")
