/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding
import java.nio.file.Files
import java.nio.file.Path

import akka.actor.{ ActorSystem, ExtendedActorSystem }

/**
 * This will work on JDK11 and JDK8 built with the enable-jfr flag (8u262+).
 *
 * For Akka JRF recordings you may need to run a publish for multi jvm tests
 * to get the ComileJDK9 things compiled.
 */
class FlightRecording(system: ActorSystem) {

  private val dynamic = system.asInstanceOf[ExtendedActorSystem].dynamicAccess
  private val recording =
    dynamic.createInstanceFor[AnyRef]("jdk.jfr.Recording", Nil).toOption
  private val clazz = recording.map(_.getClass)
  private val startMethod = clazz.map(_.getDeclaredMethod("start"))
  private val stopMethod = clazz.map(_.getDeclaredMethod("stop"))
  private val dumpMethod = clazz.map(_.getDeclaredMethod("dump", classOf[Path]))

  def start() = {
    for {
      r <- recording
      m <- startMethod
    } yield m.invoke(r)
  }

  def endAndDump(location: Path) = {
    // Make sure parent directory exists
    if (location.getParent != null)
      Files.createDirectories(location.getParent)

    for {
      r <- recording
      stop <- stopMethod
      dump <- dumpMethod
    } yield {
      stop.invoke(r)
      dump.invoke(r, location)
    }
  }
}
