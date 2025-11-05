/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import scala.util.Try

import akka.actor.ActorSystem

/**
 * Simple to file logger for benchmark results. Will log relevant settings first to make sure
 * results can be understood later.
 */
trait BenchmarkFileReporter {
  def testName: String
  def reportResults(result: String): Unit
  def close(): Unit
}
object BenchmarkFileReporter {
  val targetDirectory = {
    val target = new File("akka-stream-tests/target/benchmark-results")
    target.mkdirs()
    target
  }

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")

  def apply(test: String, system: ActorSystem): BenchmarkFileReporter = {
    val settingsToReport =
      Seq(
        "akka.test.MaxThroughputSpec.totalMessagesFactor",
        "akka.test.MaxThroughputSpec.real-message",
        "akka.test.LatencySpec.totalMessagesFactor",
        "akka.test.LatencySpec.repeatCount",
        "akka.test.LatencySpec.real-message",
        "akka.remote.artery.enabled",
        "akka.remote.artery.advanced.inbound-lanes",
        "akka.remote.artery.advanced.buffer-pool-size",
        "akka.remote.artery.advanced.aeron.idle-cpu-level",
        "akka.remote.artery.advanced.aeron.embedded-media-driver",
        "akka.remote.default-remote-dispatcher.throughput",
        "akka.remote.default-remote-dispatcher.fork-join-executor.parallelism-factor",
        "akka.remote.default-remote-dispatcher.fork-join-executor.parallelism-min",
        "akka.remote.default-remote-dispatcher.fork-join-executor.parallelism-max")
    apply(test, system, settingsToReport)
  }

  def apply(test: String, system: ActorSystem, settingsToReport: Seq[String]): BenchmarkFileReporter =
    new BenchmarkFileReporter {
      override val testName = test

      val gitCommit = {
        import sys.process._
        Try("git describe".!!.trim).getOrElse("[unknown]")
      }
      val testResultFile: File = {
        val timestamp = formatter.format(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()))
        val fileName = s"$timestamp-Artery-$testName-$gitCommit-results.txt"
        new File(targetDirectory, fileName)
      }
      val config = system.settings.config

      val fos = Files.newOutputStream(testResultFile.toPath)
      reportResults(s"Git commit: $gitCommit")

      settingsToReport.foreach(reportSetting)

      def reportResults(result: String): Unit = synchronized {
        println(result)
        fos.write(result.getBytes("utf8"))
        fos.write('\n')
        fos.flush()
      }

      def reportSetting(name: String): Unit = {
        val value = if (config.hasPath(name)) config.getString(name) else "[unset]"
        reportResults(s"$name: $value")
      }

      def close(): Unit = fos.close()
    }
}
