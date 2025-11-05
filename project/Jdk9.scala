/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://akka.io>
 */

package akka

import sbt._
import sbt.Keys._

object Jdk9 extends AutoPlugin {
  import JdkOptions.notOnJdk8

  val CompileJdk9 = config("CompileJdk9").extend(Compile)

  val TestJdk9 = config("TestJdk9").extend(Test).extend(CompileJdk9)

  val SCALA_SOURCE_DIRECTORY = "scala-jdk-9"
  val SCALA_TEST_SOURCE_DIRECTORY = "scala-jdk9-only"
  val JAVA_SOURCE_DIRECTORY = "java-jdk-9"
  val JAVA_TEST_SOURCE_DIRECTORY = "java-jdk9-only"

  val compileJdk9Settings = Seq(
    // following the scala-2.12, scala-sbt-1.0, ... convention
    unmanagedSourceDirectories := notOnJdk8(
        Seq(
          (Compile / sourceDirectory).value / SCALA_SOURCE_DIRECTORY,
          (Compile / sourceDirectory).value / JAVA_SOURCE_DIRECTORY)),
    scalacOptions := AkkaBuild.DefaultScalacOptions.value ++ notOnJdk8(Seq("-release", "11")),
    javacOptions := AkkaBuild.DefaultJavacOptions ++ notOnJdk8(Seq("--release", "11")))

  val testJdk9Settings = Seq(
    // following the scala-2.12, scala-sbt-1.0, ... convention
    unmanagedSourceDirectories := notOnJdk8(
        Seq(
          (Test / sourceDirectory).value / SCALA_TEST_SOURCE_DIRECTORY,
          (Test / sourceDirectory).value / JAVA_TEST_SOURCE_DIRECTORY)),
    scalacOptions := AkkaBuild.DefaultScalacOptions.value ++ notOnJdk8(Seq("-release", "11")),
    javacOptions := AkkaBuild.DefaultJavacOptions ++ notOnJdk8(Seq("--release", "11")),
    compile := compile.dependsOn(CompileJdk9 / compile).value,
    classpathConfiguration := TestJdk9,
    externalDependencyClasspath := (Test / externalDependencyClasspath).value)

  val compileSettings = Seq(
    // add the jdk9 products to the jar
    Compile / packageBin / mappings ++= (CompileJdk9 / products).value.flatMap(Path.allSubpaths))

  val testSettings = Seq((Test / test) := {
    (Test / test).value
    (TestJdk9 / test).value
  })

  override def trigger = noTrigger
  override def projectConfigurations = Seq(CompileJdk9)
  override lazy val projectSettings =
    inConfig(CompileJdk9)(Defaults.compileSettings) ++
    inConfig(CompileJdk9)(compileJdk9Settings) ++
    compileSettings ++
    inConfig(TestJdk9)(Defaults.testSettings) ++
    inConfig(TestJdk9)(testJdk9Settings) ++
    testSettings
}
