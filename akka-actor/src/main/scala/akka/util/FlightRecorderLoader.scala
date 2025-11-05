/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.util
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

import akka.actor.{ ClassicActorSystemProvider, ExtendedActorSystem }
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object FlightRecorderLoader {
  def load[T: ClassTag](casp: ClassicActorSystemProvider, fqcn: String, fallback: T): T = {
    val system = casp.classicSystem.asInstanceOf[ExtendedActorSystem]
    if (JavaVersion.majorVersion >= 11 && system.settings.config.getBoolean("akka.java-flight-recorder.enabled")) {
      // Dynamic instantiation to not trigger class load on earlier JDKs
      system.dynamicAccess.createInstanceFor[T](fqcn, Nil) match {
        case Success(jfr) =>
          jfr
        case Failure(ex) =>
          system.log.warning("Failed to load JFR flight recorder, falling back to noop. Exception: {}", ex.toString)
          fallback
      } // fallback if not possible to dynamically load for some reason
    } else
      // JFR not available on Java 8
      fallback
  }
}
