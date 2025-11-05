/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import sbt._
import sbt.Keys._
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import com.lightbend.sbt.publishrsync.PublishRsyncPlugin.autoImport.publishRsyncHost
import sbt.Def
import com.geirsson.CiReleasePlugin
import com.jsuereth.sbtpgp.PgpKeys.publishSigned

object Publish extends AutoPlugin {

  val defaultPublishTo = settingKey[File]("Default publish directory")

  override def trigger = allRequirements

  lazy val beforePublishTask = taskKey[Unit]("setup before publish")

  lazy val beforePublishDone = new AtomicBoolean(false)

  def beforePublish(snapshot: Boolean) = {
    if (beforePublishDone.compareAndSet(false, true)) {
      CiReleasePlugin.setupGpg()
      if (!snapshot)
        cloudsmithCredentials(validate = true)
    }
  }

  override lazy val projectSettings = Seq(
      publishRsyncHost := "akkarepo@gustav.akka.io",
      organizationName := "Lightbend Inc.",
      organizationHomepage := Some(url("https://akka.io")),
      startYear := Some(2009),
      developers := List(
          Developer(
            "akka-contributors",
            "Akka Contributors",
            "akka.official@gmail.com",
            url("https://github.com/akka/akka-core/graphs/contributors"))),
      publishMavenStyle := true,
      pomIncludeRepository := { x =>
        false
      },
      defaultPublishTo := target.value / "repository") ++ publishingSettings

  private lazy val publishingSettings: Seq[Def.Setting[_]] = {
    Def.settings(
      beforePublishTask := beforePublish(isSnapshot.value),
      publishSigned := publishSigned.dependsOn(beforePublishTask).value,
      publishTo := (if (isSnapshot.value)
                      Some("Cloudsmith API".at("https://maven.cloudsmith.io/lightbend/akka-snapshots/"))
                    else
                      Some("Cloudsmith API".at("https://maven.cloudsmith.io/lightbend/akka/"))),
      credentials ++= cloudsmithCredentials(validate = false))
  }

  def cloudsmithCredentials(validate: Boolean): Seq[Credentials] = {
    (sys.env.get("PUBLISH_USER"), sys.env.get("PUBLISH_PASSWORD")) match {
      case (Some(user), Some(password)) =>
        Seq(Credentials("Cloudsmith API", "maven.cloudsmith.io", user, password))
      case _ =>
        if (validate)
          throw new Exception("Publishing credentials expected in `PUBLISH_USER` and `PUBLISH_PASSWORD`.")
        else
          Nil
    }
  }
}

/**
 * For projects that are not to be published.
 */
object NoPublish extends AutoPlugin {
  override def requires = plugins.JvmPlugin

  override def projectSettings =
    Seq(publish / skip := true, Compile / doc / sources := Seq.empty)
}
