/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

import akka.Jdk9.CompileJdk9
import akka.{ Jdk9, MiMa }
import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport.{ mimaCurrentClassfiles, mimaReportBinaryIssues }
import sbt._
import sbt.Keys._
import sbt.PluginTrigger.AllRequirements
import sbt.{ AutoPlugin, Compile, Def, IO }

object Jdk9MiMa extends AutoPlugin {

  val prepForMima = taskKey[File]("Prepare a merged class directory for mima")

  override def projectSettings: Seq[Def.Setting[_]] =
    Seq(
      // compiled jdk9+ only sources end up in a separate class directory
      // so for those we now have two class directories under target but mima only understands one
      // so merge regular class directory with jdk9 class directory and have mima check those
      prepForMima := {
        (Compile / compile).value
        (CompileJdk9 / compile).value
        val destination = file((Compile / classDirectory).value.getParent) / "classesForMima"
        val log = streams.value.log
        streams.value.log.debug("Special handling of JDK9 only classes for MiMa check triggered")
        val allClassDirectories = (Compile / productDirectories).value ++ (CompileJdk9 / productDirectories).value
        if (destination.exists()) {
          IO.delete(destination)
        }
        destination.mkdirs()
        allClassDirectories.foreach { directory =>
          IO.copyDirectory(directory, destination)
        }
        destination
      },
      mimaCurrentClassfiles := prepForMima.value)

  override def trigger = AllRequirements
  override def requires = MiMa && Jdk9

}
