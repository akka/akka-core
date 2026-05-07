/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.{ headerMappings, headerSources, HeaderFileType }
import sbt.Keys.sourceDirectory
import sbt.{ Compile, Def, Test, inConfig, _ }

object CopyrightHeaderForProtobuf extends CopyrightHeader {
  override protected def headerMappingSettings: Seq[Def.Setting[_]] = {
    super.headerMappingSettings
    Seq(Compile, Test).flatMap { config =>
      inConfig(config) {
        Seq(
          config / headerSources ++=
            (((config / sourceDirectory).value / "protobuf") ** "*.proto").get,
          headerMappings := headerMappings.value ++ Map(HeaderFileType("proto") -> cStyleComment))
      }
    }
  }
}
