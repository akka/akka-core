import scala.collection.Seq

name := "local-scala"
version := "1.0"
scalaVersion := "2.13.17"

// Note: this default isn't really used anywhere so not important to bump
lazy val akkaVersion = sys.props.getOrElse("akka.version", "2.10.9")

// useful for investigations: sbt nativeImageRunAgent

fork := true

javacOptions += "-parameters"

// GraalVM native image build
enablePlugins(NativeImagePlugin)
nativeImageJvm := "graalvm-community"
nativeImageVersion := "21.0.2"
nativeImageOptions := Seq(
  "--no-fallback",
  "--verbose",
  "-Dakka.native-image.debug=true",
  "--initialize-at-build-time=ch.qos.logback")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.5.8",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test)
