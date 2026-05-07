/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import sbt._
import sbt.Keys._

/**
 * Sigar java agent injection build settings.
 */
object SigarLoader {

  import Dependencies.Provided.sigarLoader

  /** Enable Sigar java agent injection during tests. */
  lazy val sigarTestEnabled = sys.props.get("akka.test.sigar").getOrElse("false").toBoolean

  lazy val sigarArtifact = TaskKey[File]("sigar-artifact", "Location of Sigar java agent jar.")

  lazy val sigarOptions = TaskKey[String]("sigar-options", "JVM command line options for Sigar java agent.")

  lazy val sigarFolder = SettingKey[File]("sigar-folder", "Location of native library extracted by Sigar java agent.")

  /** Sigar agent command line option property. */
  val sigarFolderProperty = "kamon.sigar.folder"

  // TODO remove Sigar form test:test* classpath, it is provided by Sigar agent.
  lazy val sigarSettings = {
    Seq(
      //
      // Prepare Sigar agent options.
      sigarArtifact := {
        val report = update.value
        val artifactList =
          report.matching(moduleFilter(organization = sigarLoader.organization, name = sigarLoader.name))
        require(artifactList.size == 1, "Expecting single artifact, while found: " + artifactList)
        artifactList.head
      },
      sigarFolder := target.value / "native",
      sigarOptions := "-javaagent:" + sigarArtifact.value + "=" + sigarFolderProperty + "=" + sigarFolder.value,
      //
      Test / fork := true) ++ (// Invoke Sigar agent at JVM init time, to extract and load native Sigar library.
    if (sigarTestEnabled) Seq(Test / javaOptions += sigarOptions.value)
    else Seq())
  }

}
