/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import java.io.File

import com.typesafe.config.ConfigFactory
import sbt.ConsoleLogger

// used by JavaFormatter
class ProjectFileIgnoreSupport(ignoreConfigFile: File, descriptor: String) {
  private val stdoutLogger = ConsoleLogger(System.out)

  private val javaSourceDirectories = Set("java")

  private val scalaSourceDirectories = Set("scala")

  private lazy val ignoreConfig = {
    require(
      ignoreConfigFile.exists(),
      s"Expected ignore configuration for $descriptor at ${ignoreConfigFile.getAbsolutePath} but was missing")
    ConfigFactory.parseFile(ignoreConfigFile)
  }

  private lazy val ignoredFiles: Set[String] = {
    import scala.jdk.CollectionConverters._
    stdoutLogger.debug(s"Loading ignored-files from $ignoreConfigFile:[${ignoreConfig.origin().url().toURI.getPath}]")
    ignoreConfig.getStringList("ignored-files").asScala.toSet
  }

  private lazy val ignoredPackages: Set[String] = {
    import scala.jdk.CollectionConverters._
    stdoutLogger.debug(
      s"Loading ignored-packages from $ignoreConfigFile:[${ignoreConfig.origin().url().toURI.getPath}]")
    ignoreConfig.getStringList("ignored-packages").asScala.toSet
  }

  def isIgnoredByFileOrPackages(file: File): Boolean =
    isIgnoredByFile(file) || isIgnoredByPackages(file)

  private def isIgnoredByFile(file: File): Boolean = {
    val ignoredByFile = ignoredFiles(file.getName)
    if (ignoredByFile) {
      stdoutLogger.debug(s"$descriptor ignored file with file name:${file.getName} file:[${file.toPath}]")
    }
    ignoredByFile
  }

  private def isIgnoredByPackages(file: File): Boolean = {
    val ignoredByPackages = ignoredPackages.exists(pkg => {
      getPackageName(file.toURI.toString) match {
        case Some(packageName) =>
          val ignored = packageName.startsWith(pkg)
          if (ignored) {
            stdoutLogger.debug(
              s"$descriptor ignored file with pkg:$pkg for package:$packageName file:[${file.toPath}] ")
          }
          ignored
        case None => false
      }
    })
    ignoredByPackages
  }

  private def getPackageName(fileName: String): Option[String] = {
    def getPackageName0(sourceDirectories: Set[String]): String = {
      import java.io.{ File => JFile }
      val packageName = fileName
        .split(JFile.separatorChar)
        .dropWhile(part => !sourceDirectories(part))
        .drop(1)
        .dropRight(1)
        .mkString(".")
      packageName
    }

    fileName.split('.').lastOption match {
      case Some(fileType) =>
        fileType match {
          case "java" =>
            Option(getPackageName0(javaSourceDirectories))
          case "scala" =>
            Option(getPackageName0(scalaSourceDirectories))
          case _ => None
        }
      case None => None
    }
  }
}
