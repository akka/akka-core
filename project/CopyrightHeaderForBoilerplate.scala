/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import de.heikoseeberger.sbtheader.HeaderPlugin
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbt.Keys.sourceDirectory
import sbt.{ Compile, Def, Plugins, Test, inConfig, _ }
import spray.boilerplate.BoilerplatePlugin

object CopyrightHeaderForBoilerplate extends CopyrightHeader {
  override def requires: Plugins = BoilerplatePlugin && HeaderPlugin

  override protected def headerMappingSettings: Seq[Def.Setting[_]] = {
    super.headerMappingSettings
    Seq(Compile, Test).flatMap { config =>
      inConfig(config) {
        Seq(
          config / headerSources ++=
            (((config / sourceDirectory).value / "boilerplate") ** "*.template").get,
          headerMappings := headerMappings.value ++ Map(HeaderFileType("template") -> cStyleComment))
      }
    }
  }
}
