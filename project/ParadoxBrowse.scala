/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import com.lightbend.paradox.sbt.ParadoxPlugin
import com.lightbend.paradox.sbt.ParadoxPlugin.autoImport._
import sbt.Keys._
import sbt._

object ParadoxBrowse extends AutoPlugin {

  object autoImport {
    val paradoxBrowse = taskKey[Unit]("Open the docs in the default browser")
  }
  import autoImport._

  override def trigger = allRequirements
  override def requires = ParadoxPlugin

  override lazy val projectSettings = Seq(paradoxBrowse := {
    import java.awt.Desktop
    val rootDocFile = (Compile / paradox).value / "index.html"
    val log = streams.value.log
    if (Desktop.isDesktopSupported) Desktop.getDesktop.open(rootDocFile)
    else log.info(s"Couldn't open default browser, but docs are at $rootDocFile")
  })
}
