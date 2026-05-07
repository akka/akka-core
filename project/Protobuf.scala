/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import java.io.File
import java.io.PrintWriter

import scala.sys.process._

import sbt._
import sbt.util.CacheStoreFactory
import Keys._

import sbtassembly.AssemblyKeys._

object Protobuf {
  val paths = SettingKey[Seq[File]]("protobuf-paths", "The paths that contain *.proto files.")
  val outputPaths =
    SettingKey[Seq[File]]("protobuf-output-paths", "The paths where to save the generated *.java files.")
  val importPath = SettingKey[Option[File]](
    "protobuf-import-path",
    "The path that contain additional *.proto files that can be imported.")
  val protoc = SettingKey[String]("protobuf-protoc", "The path and name of the protoc executable.")
  val protocVersion = SettingKey[String]("protobuf-protoc-version", "The version of the protoc executable.")
  val generate = TaskKey[Unit]("protobuf-generate", "Compile the protobuf sources and do all processing.")

  lazy val settings: Seq[Setting[_]] = Seq(
    paths := Seq((Compile / sourceDirectory).value, (Test / sourceDirectory).value).map(_ / "protobuf"),
    outputPaths := Seq((Compile / sourceDirectory).value, (Test / sourceDirectory).value).map(_ / "java"),
    importPath := None,
    // this keeps intellij happy for files that use the shaded protobuf
    Compile / unmanagedJars += (LocalProject("akka-protobuf-v3") / assembly).value,
    protoc := "protoc",
    protocVersion := "25.0", // 3.25.5, also sync with protobufJavaVersion in Dependencies.scala
    generate := {
      val sourceDirs = paths.value
      val targetDirs = outputPaths.value
      val log = streams.value.log

      if (sourceDirs.size != targetDirs.size)
        sys.error(
          s"Unbalanced number of paths and destination paths!\nPaths: $sourceDirs\nDestination Paths: $targetDirs")

      if (sourceDirs.exists(_.exists)) {
        val cmd = protoc.value

        checkProtocVersion(cmd, protocVersion.value, log)

        val base = baseDirectory.value
        val sources = base / "src"
        val targets = target.value
        val cache = targets / "protoc" / "cache"

        (sourceDirs.zip(targetDirs)).map {
          case (src, dst) =>
            val relative = src
              .relativeTo(sources)
              .getOrElse(throw new Exception(s"path $src is not a in source tree $sources"))
              .toString
            val tmp = targets / "protoc" / relative
            IO.delete(tmp)
            generate(cmd, src, tmp, log, importPath.value)
            transformDirectory(
              tmp,
              dst,
              _ => true,
              transformFile(
                _.replace("com.google.protobuf", "akka.protobufv3.internal")
                // this is the one thing that protobufGenerate doesn't fully qualify and causes
                // api doc generation to fail
                  .replace(
                    "UnusedPrivateParameter",
                    "akka.protobufv3.internal.GeneratedMessageV3.UnusedPrivateParameter")),
              cache,
              log)
        }
      }
    })

  private def callProtoc[T](protoc: String, args: Seq[String], log: Logger, thunk: (ProcessBuilder, Logger) => T): T =
    try {
      val proc = Process(protoc, args)
      thunk(proc, log)
    } catch {
      case e: Exception =>
        throw new RuntimeException("error while executing '%s' with args: %s".format(protoc, args.mkString(" ")), e)
    }

  private def checkProtocVersion(protoc: String, protocVersion: String, log: Logger): Unit = {
    val res = callProtoc(protoc, Seq("--version"), log, { (p, l) =>
      p !! l
    })
    val version = res.split(" ").last.trim
    if (version != protocVersion) {
      sys.error("Wrong protoc version! Expected %s but got %s".format(protocVersion, version))
    }
  }

  private def generate(protoc: String, srcDir: File, targetDir: File, log: Logger, importPath: Option[File]): Unit = {
    val protoFiles = (srcDir ** "*.proto").get
    if (srcDir.exists)
      if (protoFiles.isEmpty)
        log.info("Skipping empty source directory %s".format(srcDir))
      else {
        targetDir.mkdirs()

        log.info("Generating %d protobuf files from %s to %s".format(protoFiles.size, srcDir, targetDir))
        protoFiles.foreach { proto =>
          log.info("Compiling %s".format(proto))
        }

        val protoPathArg = importPath match {
          case None    => Nil
          case Some(p) => Seq("--proto_path", p.absolutePath)
        }

        val exitCode = callProtoc(
          protoc,
          Seq("-I" + srcDir.absolutePath, "--java_out=%s".format(targetDir.absolutePath)) ++
          protoPathArg ++ protoFiles.map(_.absolutePath),
          log, { (p, l) =>
            p ! l
          })
        if (exitCode != 0)
          sys.error("protoc returned exit code: %d".format(exitCode))
      }
  }

  /**
   * Create a transformed version of all files in a directory, given a predicate and a transform function for each file. From sbt-site
   */
  private def transformDirectory(
      sourceDir: File,
      targetDir: File,
      transformable: File => Boolean,
      transform: (File, File) => Unit,
      cache: File,
      log: Logger): File = {
    val runTransform = FileFunction.cached(CacheStoreFactory(cache), FilesInfo.hash, FilesInfo.exists) {
      (in: ChangeReport[File], out: ChangeReport[File]) =>
        val map = Path.rebase(sourceDir, targetDir)
        if (in.removed.nonEmpty || in.modified.nonEmpty) {
          log.info("Preprocessing directory %s...".format(sourceDir))
          for (source <- in.removed; target <- map(source)) {
            IO.delete(target)
          }
          val updated = for (source <- in.modified; target <- map(source)) yield {
            if (source.isFile) {
              if (transformable(source)) transform(source, target)
              else IO.copyFile(source, target)
            }
            target
          }
          log.info("Directory preprocessed: " + targetDir)
          updated
        } else Set.empty
    }
    val sources = (sourceDir.allPaths).get.toSet
    runTransform(sources)
    targetDir
  }

  /**
   * Transform a file, line by line.
   */
  def transformFile(transform: String => String)(source: File, target: File): Unit = {
    IO.reader(source) { reader =>
      IO.writer(target, "", IO.defaultCharset) { writer =>
        val pw = new PrintWriter(writer)
        IO.foreachLine(reader) { line =>
          pw.println(transform(line))
        }
      }
    }
  }

}
