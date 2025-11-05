/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

import akka.ProjectFileIgnoreSupport
import com.lightbend.sbt.JavaFormatterPlugin
import sbt.{ AutoPlugin, PluginTrigger, Plugins }

object JavaFormatter extends AutoPlugin {

  override def trigger = PluginTrigger.AllRequirements

  override def requires: Plugins = JavaFormatterPlugin

  private val ignoreConfigFileName: String = ".sbt-java-formatter.conf"
  private val descriptor: String = "sbt-java-formatter"

  private val formatOnCompile = !sys.props.contains("akka.no.discipline")

  import JavaFormatterPlugin.autoImport._
  import sbt.Keys._
  import sbt._
  import sbt.io._

  override def projectSettings: Seq[Def.Setting[_]] =
    Seq(
      //below is for sbt java formatter
      javafmt / excludeFilter := {
        val ignoreSupport =
          new ProjectFileIgnoreSupport((ThisBuild / baseDirectory).value / ignoreConfigFileName, descriptor)
        val simpleFileFilter = new SimpleFileFilter(file => ignoreSupport.isIgnoredByFileOrPackages(file))
        simpleFileFilter || (javafmt / excludeFilter).value
      },
      javafmtOnCompile := formatOnCompile)
}
