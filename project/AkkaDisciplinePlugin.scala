/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import sbt._
import Keys.{ scalacOptions, _ }
import sbt.plugins.JvmPlugin

object AkkaDisciplinePlugin extends AutoPlugin {

  override def trigger: PluginTrigger = allRequirements
  override def requires: Plugins = JvmPlugin
  override lazy val projectSettings = disciplineSettings

  // allow toggling for pocs/exploration of ideas without discpline
  val enabled = !sys.props.contains("akka.no.discipline")

  // We allow warnings in docs to get the 'snippets' right
  val nonFatalJavaWarningsFor = Set(
    // for sun.misc.Unsafe and AbstractScheduler
    "akka-actor",
    // references to deprecated PARSER fields in generated message formats?
    "akka-actor-typed-tests",
    // references to deprecated PARSER fields in generated message formats?
    "akka-cluster-typed",
    "akka-protobuf-v3",
    // references to deprecated PARSER fields in generated message formats?
    "akka-remote",
    // references to deprecated PARSER fields in generated message formats?
    "akka-distributed-data",
    // references to deprecated PARSER fields in generated message formats?
    "akka-cluster-sharding-typed",
    // references to deprecated PARSER fields in generated message formats?
    "akka-persistence-typed",
    // references to deprecated PARSER fields in generated message formats?
    "akka-persistence-query",
    "akka-docs",
    // use varargs of `Graph` in alsoTo and etc operators
    "akka-stream-tests")

  val looseProjects = Set(
    "akka-actor",
    "akka-actor-testkit-typed",
    "akka-actor-tests",
    "akka-actor-typed",
    "akka-actor-typed-tests",
    "akka-bench-jmh",
    "akka-cluster",
    "akka-cluster-metrics",
    "akka-cluster-sharding",
    "akka-cluster-sharding-typed",
    "akka-distributed-data",
    "akka-docs",
    "akka-persistence",
    "akka-persistence-tck",
    "akka-persistence-typed",
    "akka-persistence-query",
    "akka-remote",
    "akka-remote-tests",
    "akka-stream",
    "akka-stream-testkit",
    "akka-stream-tests",
    "akka-stream-tests-tck",
    "akka-testkit")

  // cat=lint-deprecation: we want to keep using both Java and Scala deprecation annotations
  val defaultScala2Options = "-Wconf:any:e,cat=lint-deprecation:s"
  val defaultScala2TestOptions = "-Wconf:any:e"

  // Set to verbose warn instead of fail because hard to get it to comply with 2.13 discipline settings
  // none of the cat or msg silences I've tried seem to work, but any:v does
  val defaultScala3Options = "-Wconf:any:verbose"

  lazy val nowarnSettings = Seq(
    Compile / scalacOptions += (
        if (scalaVersion.value.startsWith("3.")) defaultScala3Options
        else defaultScala2Options
      ),
    Test / scalacOptions ++= (
        if (scalaVersion.value.startsWith("3.")) Seq.empty
        else Seq(defaultScala2TestOptions)
      ),
    Compile / doc / scalacOptions := Seq())

  /**
   * We are a little less strict in docs
   */
  val docs =
    Seq(
      Compile / scalacOptions --= Seq(defaultScala2Options, defaultScala3Options),
      Compile / scalacOptions += "-Wconf:any:e,cat=unused:s,cat=deprecation:s,cat=unchecked:s",
      Test / scalacOptions --= Seq("-Xlint", "-unchecked", "-deprecation"),
      Test / scalacOptions --= Seq(defaultScala2Options, defaultScala3Options),
      Test / scalacOptions +=
        (if (scalaVersion.value.startsWith("3.")) defaultScala3Options
         else "-Wconf:cat=any:e,unused:s,cat=deprecation:s,cat=unchecked:s"),
      Compile / doc / scalacOptions := Seq())

  lazy val disciplineSettings =
    if (enabled) {
      nowarnSettings ++ Seq(
        Test / scalacOptions --= testUndiscipline,
        Compile / javacOptions ++= (
            if (scalaVersion.value.startsWith("3.")) {
              Seq()
            } else {
              if (!nonFatalJavaWarningsFor(name.value)) Seq("-Werror", "-Xlint:deprecation", "-Xlint:unchecked")
              else Seq.empty
            }
          ),
        Compile / doc / javacOptions := Seq("-Xdoclint:none"),
        Compile / scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
            case Some((2, 13)) =>
              disciplineScalac2Options -- Set(
                "-Ywarn-inaccessible",
                "-Ywarn-infer-any",
                "-Ywarn-nullary-override",
                "-Ywarn-nullary-unit",
                "-Ypartial-unification",
                "-Yno-adapted-args")
            case _ =>
              disciplineScalac3Options
          }).toSeq,
        Compile / scalacOptions --=
          (if (looseProjects.contains(name.value)) undisciplineScalacOptions.toSeq
           else Seq.empty),
        // Discipline is not needed for the docs compilation run (which uses
        // different compiler phases from the regular run), and in particular
        // '-Ywarn-unused:explicits' breaks 'sbt ++2.13.0-M5 akka-actor/doc'
        // https://github.com/akka/akka-core/issues/26119
        Compile / doc / scalacOptions --= (
            if (scalaVersion.value.startsWith("3.")) disciplineScalac3Options.toSeq
            else disciplineScalac2Options.toSeq
          ),
        // having discipline warnings in console is just an annoyance
        Compile / console / scalacOptions --= (
            if (scalaVersion.value.startsWith("3.")) disciplineScalac3Options.toSeq
            else disciplineScalac2Options.toSeq
          ),
        Test / console / scalacOptions --= (
            if (scalaVersion.value.startsWith("3.")) disciplineScalac3Options.toSeq
            else disciplineScalac2Options.toSeq
          ))
    } else {
      // we still need these in opt-out since the annotations are present
      nowarnSettings ++ Seq(Compile / scalacOptions += "-deprecation")
    }

  val testUndiscipline = Seq("-Ywarn-dead-code" // '???' used in compile only specs
  )

  /**
   * Remain visibly filtered for future code quality work and removing.
   */
  val undisciplineScalacOptions = Set("-Ywarn-numeric-widen")

  /** These options are desired, but some are excluded for the time being*/
  val disciplineScalac2Options = Set(
    "-Ywarn-numeric-widen",
    "-Yno-adapted-args",
    "-deprecation",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-unused:_",
    "-Ypartial-unification",
    "-Ywarn-extra-implicit")

  val disciplineScalac3Options = Set.empty
}
