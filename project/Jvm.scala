/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package com.typesafe.sbt.multijvm

import java.io.File
import java.lang.{ ProcessBuilder => JProcessBuilder }

import sbt._
import scala.sys.process.Process

object Jvm {
  def startJvm(
      javaBin: File,
      jvmOptions: Seq[String],
      runOptions: Seq[String],
      logger: Logger,
      connectInput: Boolean) = {
    forkJava(javaBin, jvmOptions ++ runOptions, logger, connectInput)
  }

  def forkJava(javaBin: File, options: Seq[String], logger: Logger, connectInput: Boolean) = {
    val java = javaBin.toString
    val command = (java :: options.toList).toArray
    val builder = new JProcessBuilder(command: _*)
    Process(builder).run(logger, connectInput)
  }

  /**
   * check if the current operating system is some OS
  **/
  def isOS(os: String) =
    try {
      System.getProperty("os.name").toUpperCase.startsWith(os.toUpperCase)
    } catch {
      case _: Throwable => false
    }

  /**
   * convert to proper path for the operating system
  **/
  def osPath(path: String) = if (isOS("WINDOWS")) Process(Seq("cygpath", path)).lineStream.mkString else path

  def getPodName(hostAndUser: String, sbtLogger: Logger): String = {
    val command: Array[String] =
      Array("kubectl", "get", "pods", "-l", s"host=$hostAndUser", "--no-headers", "-o", "name")
    val builder = new JProcessBuilder(command: _*)
    sbtLogger.debug("Jvm.getPodName about to run " + command.mkString(" "))
    val podName = Process(builder).!!
    sbtLogger.debug("Jvm.getPodName podName is " + podName)
    podName.stripPrefix("pod/").stripSuffix("\n")
  }

  def syncJar(jarName: String, hostAndUser: String, remoteDir: String, sbtLogger: Logger): Process = {
    // Retry up to 3 times to handle transient GKE API server connection resets
    @annotation.tailrec
    def attempt(attemptsLeft: Int): Int = {
      val podName = getPodName(hostAndUser, sbtLogger)
      val mkdirCommand: Array[String] =
        Array("kubectl", "exec", podName, "--", "/bin/bash", "-c", s"rm -rf $remoteDir && mkdir -p $remoteDir")
      sbtLogger.debug("Jvm.syncJar about to run " + mkdirCommand.mkString(" "))
      val mkdirExitCode = Process(new JProcessBuilder(mkdirCommand: _*)).run(sbtLogger, false).exitValue()
      val exitCode = if (mkdirExitCode == 0) {
        val cpCommand: Array[String] = Array("kubectl", "cp", osPath(jarName), podName + ":" + remoteDir + "/")
        sbtLogger.debug("Jvm.syncJar about to run " + cpCommand.mkString(" "))
        Process(new JProcessBuilder(cpCommand: _*)).run(sbtLogger, false).exitValue()
      } else {
        mkdirExitCode
      }
      if (exitCode == 0 || attemptsLeft <= 1) {
        exitCode
      } else {
        sbtLogger.warn(
          s"Jvm.syncJar failed (exit code $exitCode), retrying in 5s (${attemptsLeft - 1} attempt(s) left)")
        Thread.sleep(5000)
        attempt(attemptsLeft - 1)
      }
    }
    val exitCode = attempt(3)
    new Process {
      def exitValue(): Int = exitCode
      def destroy(): Unit = ()
      def isAlive: Boolean = false
    }
  }

  def forkRemoteJava(
      java: String,
      jvmOptions: Seq[String],
      appOptions: Seq[String],
      jarName: String,
      hostAndUser: String,
      remoteDir: String,
      logger: Logger,
      connectInput: Boolean,
      sbtLogger: Logger): Process = {
    val podName = getPodName(hostAndUser, sbtLogger)
    sbtLogger.debug("About to use java " + java)
    val shortJarName = new File(jarName).getName
    val javaCommand = List(List(java), jvmOptions, List("-cp", shortJarName), appOptions).flatten
    val command = Array(
      "kubectl",
      "exec",
      podName,
      "--",
      "/bin/bash",
      "-c",
      ("cd " :: (remoteDir :: (" ; " :: javaCommand))).mkString(" "))
    sbtLogger.debug("Jvm.forkRemoteJava about to run " + command.mkString(" "))
    val builder = new JProcessBuilder(command: _*)
    Process(builder).run(logger, connectInput)
  }
}

class JvmBasicLogger(name: String) extends BasicLogger {
  def jvm(message: String) = "[%s] %s".format(name, message)

  def log(level: Level.Value, message: => String) = System.out.synchronized {
    System.out.println(jvm(message))
  }

  def trace(t: => Throwable) = System.out.synchronized {
    val traceLevel = getTrace
    if (traceLevel >= 0) System.out.print(StackTrace.trimmed(t, traceLevel))
  }

  def success(message: => String) = log(Level.Info, message)
  def control(event: ControlEvent.Value, message: => String) = log(Level.Info, message)

  def logAll(events: Seq[LogEvent]) = System.out.synchronized { events.foreach(log) }
}

final class JvmLogger(name: String) extends JvmBasicLogger(name)
