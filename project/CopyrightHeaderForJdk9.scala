/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headerSources
import sbt.Keys.sourceDirectory
import sbt.{ Compile, Def, Test, _ }

object CopyrightHeaderForJdk9 extends CopyrightHeader {

  override protected def headerMappingSettings: Seq[Def.Setting[_]] = {
    super.headerMappingSettings
    import Jdk9._
    Seq(
      Compile / headerSources ++=
        (((Compile / sourceDirectory).value / SCALA_SOURCE_DIRECTORY) ** "*.scala").get,
      Test / headerSources ++=
        (((Test / sourceDirectory).value / SCALA_TEST_SOURCE_DIRECTORY) ** "*.scala").get,
      Compile / headerSources ++=
        (((Compile / sourceDirectory).value / JAVA_SOURCE_DIRECTORY) ** "*.java").get,
      Test / headerSources ++=
        (((Test / sourceDirectory).value / JAVA_TEST_SOURCE_DIRECTORY) ** "*.java").get)
  }
}
