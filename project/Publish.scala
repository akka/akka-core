/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka

import sbt._
import sbt.Keys._
import java.io.File
import com.lightbend.sbt.publishrsync.PublishRsyncPlugin.autoImport.publishRsyncHost
import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

object Publish extends AutoPlugin {

  val defaultPublishTo = settingKey[File]("Default publish directory")

  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    publishTo := Some(akkaPublishTo.value),
    publishRsyncHost := "akkarepo@gustav.akka.io",
    credentials ++= akkaCredentials,
    organizationName := "Lightbend Inc.",
    organizationHomepage := Some(url("https://www.lightbend.com")),
    sonatypeProfileName := "com.typesafe",
    startYear := Some(2009),
    developers := List(
        Developer(
          "akka-contributors",
          "Akka Contributors",
          "akka.official@gmail.com",
          url("https://github.com/akka/akka/graphs/contributors"))),
    publishMavenStyle := true,
    pomIncludeRepository := { x =>
      false
    },
    defaultPublishTo := target.value / "repository")

  private def akkaPublishTo = Def.setting {
    val key = new java.io.File(
      Option(System.getProperty("akka.gustav.key"))
        .getOrElse(System.getProperty("user.home") + "/.ssh/id_rsa_gustav.pem"))
    if (isSnapshot.value)
      Resolver.sftp("Akka snapshots", "gustav.akka.io", "/home/akkarepo/www/snapshots").as("akkarepo", key)
    else
      Opts.resolver.sonatypeStaging
  }

  private def akkaCredentials: Seq[Credentials] =
    Option(System.getProperty("akka.publish.credentials")).map(f => Credentials(new File(f))).toSeq
}

/**
 * For projects that are not to be published.
 */
object NoPublish extends AutoPlugin {
  override def requires = plugins.JvmPlugin

  override def projectSettings =
    Seq(publish / skip := true, Compile / doc / sources := Seq.empty)
}
