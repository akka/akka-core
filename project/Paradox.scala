/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import com.lightbend.paradox.sbt.ParadoxPlugin
import com.lightbend.paradox.sbt.ParadoxPlugin.autoImport._
import com.lightbend.paradox.apidoc.ApidocPlugin
import com.lightbend.sbt.publishrsync.PublishRsyncPlugin.autoImport._
import sbt.Keys._
import sbt._

import scala.concurrent.duration._

object Paradox {
  private val MajorMinorRegexp = """(\d+\.\d+).*""".r

  val propertiesSettings = Seq(Compile / paradoxProperties ++= {
    val akkaMajorMinorVersion = {
      version.value match {
        case MajorMinorRegexp(minor) => minor
        case surprisingVersion       => surprisingVersion
      }
    }
    Map(
      "canonical.base_url" -> "https://doc.akka.io/libraries/akka-core/current",
      "github.base_url" -> GitHub
        .url(version.value), // for links like this: @github[#1](#1) or @github[83986f9](83986f9)
      "extref.akka.http.base_url" -> "https://doc.akka.io/libraries/akka-http/current/%s",
      "extref.akka-management.base_url" -> "https://doc.akka.io/libraries/akka-management/current/%s",
      "extref.akka-projection.base_url" -> "https://doc.akka.io/libraries/akka-projection/current/%s",
      "extref.akka-distributed-cluster.base_url" -> "https://doc.akka.io/libraries/akka-distributed-cluster/current/%s",
      "extref.akka-edge.base_url" -> "https://doc.akka.io/libraries/akka-edge/current/%s",
      "extref.platform-guide.base_url" -> "https://doc.akka.io/libraries/guide/%s",
      "extref.wikipedia.base_url" -> "https://en.wikipedia.org/wiki/%s",
      "extref.github.base_url" -> (GitHub.url(version.value) + "/%s"), // for links to our sources
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka-core/$akkaMajorMinorVersion",
      "scaladoc.akka.http.base_url" -> "https://doc.akka.io/api/akka-http/current",
      "javadoc.java.base_url" -> "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/",
      "javadoc.java.link_style" -> "direct",
      "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka-core/$akkaMajorMinorVersion",
      "javadoc.akka.link_style" -> "direct",
      "javadoc.akka.http.base_url" -> "https://doc.akka.io/japi/akka-http/current",
      "javadoc.akka.http.link_style" -> "frames",
      "javadoc.com.fasterxml.jackson.annotation.base_url" -> "https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-annotations/latest/",
      "javadoc.com.fasterxml.jackson.annotation.link_style" -> "direct",
      "javadoc.com.fasterxml.jackson.databind.base_url" -> "https://javadoc.io/doc/com.fasterxml.jackson.core/jackson-databind/latest/",
      "javadoc.com.fasterxml.jackson.databind.link_style" -> "direct",
      "javadoc.com.google.protobuf.base_url" -> "https://javadoc.io/doc/com.google.protobuf/protobuf-java/latest/",
      "javadoc.com.google.protobuf.link_style" -> "direct",
      "javadoc.com.typesafe.config.base_url" -> "https://javadoc.io/doc/com.typesafe/config/latest/",
      "javadoc.com.typesafe.config.link_style" -> "direct",
      "javadoc.org.slf4j.base_url" -> "https://www.javadoc.io/doc/org.slf4j/slf4j-api/latest/org.slf4j",
      "javadoc.org.slf4j.link_style" -> "direct",
      "scala.version" -> scalaVersion.value,
      "scala.binary.version" -> scalaBinaryVersion.value,
      "akka.version" -> version.value,
      "scalatest.version" -> Dependencies.scalaTestVersion,
      "sigar_loader.version" -> "1.6.6-rev002",
      "algolia.docsearch.app_id" -> "XUXZ6LW9B1",
      "algolia.docsearch.api_key" -> "5b6260148e92f7c5e38338fcf7eaa3e0",
      "algolia.docsearch.index_name" -> "akka_docs",
      "google.analytics.account" -> "UA-21117439-1",
      "google.analytics.domain.name" -> "akka.io",
      "signature.akka.base_dir" -> (ThisBuild / baseDirectory).value.getAbsolutePath,
      "aeron_version" -> Dependencies.aeronVersion,
      "netty_version" -> Dependencies.nettyVersion,
      "logback_version" -> Dependencies.logbackVersion)
  })

  val rootsSettings = Seq(
    paradoxRoots := List(
        "index.html",
        // Page that recommends Alpakka:
        "camel.html",
        // not linked
        "fault-tolerance-sample.html",
        "project/migration-guide-2.4.x-2.5.x.html"))

  // FIXME https://github.com/lightbend/paradox/issues/350
  // Exclusions from direct compilation for includes dirs/files not belonging in a TOC
  val includesSettings = Seq(
    (Compile / paradoxMarkdownToHtml / excludeFilter) := (Compile / paradoxMarkdownToHtml / excludeFilter).value ||
      ParadoxPlugin.InDirectoryFilter((Compile / paradox / sourceDirectory).value / "includes"),
    // Links are interpreted relative to the page the snippet is included in,
    // instead of relative to the place where the snippet is declared.
    (Compile / paradoxMarkdownToHtml / excludeFilter) := (Compile / paradoxMarkdownToHtml / excludeFilter).value ||
      ParadoxPlugin.InDirectoryFilter((Compile / paradox / sourceDirectory).value / "includes.html"))

  val groupsSettings = Seq(
    Compile / paradoxGroups := Map("Language" -> Seq("Scala", "Java"), "BuildTool" -> Seq("sbt", "Maven", "Gradle")))

  val parsingSettings = Seq(Compile / paradoxParsingTimeout := 5.seconds)

  val settings =
    propertiesSettings ++
    rootsSettings ++
    includesSettings ++
    groupsSettings ++
    parsingSettings ++
    Seq(
      Compile / paradox / name := "Akka",
      resolvers += Resolver.jcenterRepo,
      ApidocPlugin.autoImport.apidocRootPackage := "akka",
      publishRsyncArtifacts += {
        val releaseVersion = if (isSnapshot.value) "snapshot" else version.value
        ((Compile / paradox).value -> s"www/libraries/akka-core/$releaseVersion")
      })
}
