/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import sbt._
import sbt.Keys._
import sbtdynver.DynVerPlugin.autoImport.dynverGitDescribeOutput

/**
 * Generate version.conf and akka/Version.scala files based on the version setting.
 */
object VersionGenerator {

  val settings: Seq[Setting[_]] = inConfig(Compile)(
    Seq(
      resourceGenerators += generateVersion(resourceManaged, _ / "version.conf", """|akka.version = "%s"
         |akka.versionReleaseDateMillis = %d
         |"""),
      sourceGenerators += generateVersion(
          sourceManaged,
          _ / "akka" / "Version.scala",
          """|package akka
         |
         |import akka.annotation.InternalApi
         |
         |object Version {
         |  /** INTERNAL API: Compile time constant. */
         |  @InternalApi private[akka] final val Current = "%s"
         |
         |  val current: String = Current
         |
         |  /** INTERNAL API: Build date as epoch millis. Compile time constant. */
         |  @InternalApi private[akka] final val BuildDate = %dL
         |}
         |""")))

  def generateVersion(dir: SettingKey[File], locate: File => File, template: String) = Def.task[Seq[File]] {
    val file = locate(dir.value)

    import scala.sys.process._
    val buildDate = dynverGitDescribeOutput.value match {
      case Some(output) if output.ref.isTag && output.isCleanAfterTag =>
        Seq("git", "tag", "-l", output.ref.value, "--format=%(creatordate:unix)").!!.trim.toLong * 1000
      case Some(output) if !output.isDirty && !output.commitSuffix.isEmpty =>
        Seq("git", "show", output.commitSuffix.sha, "-s", "--format=%ct").!!.trim.toLong * 1000
      case _ =>
        System.currentTimeMillis()
    }

    val content = template.stripMargin.format(version.value, buildDate)

    if (!file.exists || IO.read(file) != content) IO.write(file, content)
    Seq(file)
  }

}
