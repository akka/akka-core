/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import java.io.PrintWriter
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import sbt.{ Def, _ }
import Keys._
import sbt.internal.{ AppenderSupplier, LogManager }
import sbt.internal.util.ConsoleOut

object AddLogTimestamps extends AutoPlugin {
  val enableTimestamps: Boolean = CliOption("akka.log.timestamps", false).get

  override def requires: Plugins = plugins.JvmPlugin
  override def trigger: PluginTrigger = allRequirements

  private val UTC = ZoneId.of("UTC")

  override def projectSettings: Seq[Def.Setting[_]] = {
    logManager := {
      val original = logManager.value

      if (enableTimestamps) {
        val myOut = new PrintWriter(System.out) {
          val dateTimeFormat = DateTimeFormatter.ofPattern("MM-dd HH:mm:ss.SSS")
          var lastWasNewline = true

          override def print(s: String): Unit = {
            maybePrintTimestamp()

            super.print(s)

            lastWasNewline = s.endsWith("\n")
          }

          override def println(): Unit = {
            super.println()
            lastWasNewline = true
          }

          override def println(x: String): Unit = {
            maybePrintTimestamp()

            super.println(x)
            lastWasNewline = true
          }

          private def maybePrintTimestamp(): Unit =
            if (lastWasNewline) {
              super.print('[')
              super.print(dateTimeFormat.format(LocalDateTime.now(UTC)))
              super.print("] ")
              lastWasNewline = false
            }
        }

        val myLogger = ConsoleOut.printWriterOut(myOut)

        LogManager.defaults(extraAppenders.value, myLogger)
      } else
        original
    }
  }
}
