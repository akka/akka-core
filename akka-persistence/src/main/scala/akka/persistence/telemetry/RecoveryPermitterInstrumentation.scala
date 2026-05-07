/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.telemetry

import scala.collection.immutable

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalStableApi
import akka.event.Logging
import akka.util.TopologicalSort.topologicalSort
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API: Instrumentation SPI for PersistentActor.
 */
@InternalStableApi
trait RecoveryPermitterInstrumentation {

  /**
   * Record recovery permitter status - invoked after an actor has requested a permit.
   *
   * @param recoveryPermitter `ActorRef` handling the permits for this actor
   *                          system.
   * @param maxPermits        the max permits set (via configuration).
   * @param usedPermits       the number of used (issued) permits.
   * @param pendingActors     number of pending actors waiting for a permit.
   */
  def recoveryPermitterStatus(recoveryPermitter: ActorRef, maxPermits: Int, usedPermits: Int, pendingActors: Int): Unit

  /**
   * Optional dependencies for this instrumentation.
   *
   * Dependency instrumentations will always be ordered before this instrumentation.
   *
   * @return list of class names for optional instrumentation dependencies
   */
  def dependencies: immutable.Seq[String]
}

/**
 * INTERNAL API
 */
@InternalStableApi
object EmptyRecoveryPermitterInstrumentation extends EmptyRecoveryPermitterInstrumentation

/**
 * INTERNAL API
 */
@InternalStableApi
class EmptyRecoveryPermitterInstrumentation extends RecoveryPermitterInstrumentation {

  def this(system: ActorSystem) = this()

  override def recoveryPermitterStatus(
      recoveryPermitter: ActorRef,
      maxPermits: Int,
      usedPermits: Int,
      pendingActors: Int): Unit = ()

  override def dependencies: immutable.Seq[String] = Nil
}

/**
 * INTERNAL API
 */
@InternalStableApi
class RecoveryPermitterEnsemble(val instrumentations: Seq[RecoveryPermitterInstrumentation])
    extends RecoveryPermitterInstrumentation {

  override def recoveryPermitterStatus(
      recoveryPermitter: ActorRef,
      maxPermits: Int,
      usedPermits: Int,
      pendingActors: Int): Unit =
    instrumentations.foreach(_.recoveryPermitterStatus(recoveryPermitter, maxPermits, usedPermits, pendingActors))

  override def dependencies: immutable.Seq[String] =
    instrumentations.flatMap(_.dependencies)
}

/**
 * INTERNAL API
 */
@InternalStableApi
object RecoveryPermitterInstrumentationProvider
    extends ExtensionId[RecoveryPermitterInstrumentationProvider]
    with ExtensionIdProvider {
  override def get(system: ActorSystem): RecoveryPermitterInstrumentationProvider = super.get(system)

  override def get(system: ClassicActorSystemProvider): RecoveryPermitterInstrumentationProvider = super.get(system)

  override def lookup = RecoveryPermitterInstrumentationProvider

  override def createExtension(system: ExtendedActorSystem): RecoveryPermitterInstrumentationProvider =
    new RecoveryPermitterInstrumentationProvider(system)
}

/**
 * INTERNAL API
 */
@InternalStableApi
class RecoveryPermitterInstrumentationProvider(system: ExtendedActorSystem) extends Extension {
  private val fqcnConfigPath = "akka.persistence.telemetry.recovery-permitter.instrumentations"

  val instrumentation: RecoveryPermitterInstrumentation = {
    if (!system.settings.config.hasPath(fqcnConfigPath)) {
      EmptyRecoveryPermitterInstrumentation
    } else {
      val fqcns = system.settings.config.getStringList(fqcnConfigPath).asScala.toVector
      fqcns.size match {
        case 0 => EmptyRecoveryPermitterInstrumentation
        case 1 => create(fqcns.head)
        case _ =>
          val instrumentationsByFqcn = fqcns.iterator.map(fqcn => fqcn -> create(fqcn)).toMap
          val sortedNames = topologicalSort[String](fqcns, fqcn => instrumentationsByFqcn(fqcn).dependencies.toSet)
          val instrumentations = sortedNames.map(instrumentationsByFqcn).toVector
          new RecoveryPermitterEnsemble(instrumentations)
      }
    }
  }

  private def create(fqcn: String): RecoveryPermitterInstrumentation = {
    try {
      system.dynamicAccess
        .createInstanceFor[RecoveryPermitterInstrumentation](fqcn, immutable.Seq((classOf[ActorSystem], system)))
        .get
    } catch {
      case t: Throwable => // Throwable, because instrumentation failure should not cause fatal shutdown
        Logging(system.classicSystem, classOf[RecoveryPermitterInstrumentationProvider])
          .warning(t, "Cannot create instrumentation [{}]", fqcn)
        EmptyRecoveryPermitterInstrumentation
    }
  }
}
