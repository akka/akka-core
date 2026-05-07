/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.{ headerMappings, headerSources, HeaderFileType }
import sbt.Keys.baseDirectory
import sbt.{ Compile, Def, PluginTrigger, Test, inConfig, _ }

object CopyrightHeaderForBuild extends CopyrightHeader {
  override def trigger: PluginTrigger = noTrigger

  override def projectSettings: Seq[Def.Setting[_]] = {
    Seq(Compile, Test).flatMap { config =>
      inConfig(config) {
        Seq(
          config / headerSources ++= (((config / baseDirectory).value / "project") ** "*.scala").get,
          headerMappings := headerMappings.value ++ Map(HeaderFileType.scala -> cStyleComment))
      }
    }
  }
}
