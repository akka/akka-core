/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.jdk.DurationConverters._

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.Deploy
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalApi
import akka.dispatch.Dispatchers
import akka.pattern.ask
import akka.stream.impl.MaterializerGuardian
import akka.util.Timeout

/**
 * The system materializer is a default materializer to use for most cases running streams, it is a single instance
 * per actor system that is tied to the lifecycle of that system.
 *
 * Not intended to be manually used in user code.
 */
object SystemMaterializer extends ExtensionId[SystemMaterializer] with ExtensionIdProvider {
  override def get(system: ActorSystem): SystemMaterializer = super.get(system)
  override def get(system: ClassicActorSystemProvider): SystemMaterializer = super.get(system)

  override def lookup = SystemMaterializer

  override def createExtension(system: ExtendedActorSystem): SystemMaterializer =
    new SystemMaterializer(system)
}

final class SystemMaterializer(system: ExtendedActorSystem) extends Extension {
  private val systemMaterializerPromise = Promise[Materializer]()

  // load these here so we can share the same instance across materializer guardian and other uses
  /**
   * INTERNAL API
   */
  @InternalApi @nowarn("msg=deprecated")
  private[akka] val materializerSettings = ActorMaterializerSettings(system)

  private implicit val materializerTimeout: Timeout =
    system.settings.config.getDuration("akka.stream.materializer.creation-timeout").toScala

  @InternalApi
  private val materializerGuardian = system.systemActorOf(
    MaterializerGuardian
      .props(systemMaterializerPromise, materializerSettings)
      // #28037 run on internal dispatcher to make sure default dispatcher starvation doesn't stop materializer creation
      .withDispatcher(Dispatchers.InternalDispatcherId)
      .withDeploy(Deploy.local),
    "Materializers")

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def createAdditionalSystemMaterializer(): Materializer = {
    val started =
      (materializerGuardian ? MaterializerGuardian.StartMaterializer()).mapTo[MaterializerGuardian.MaterializerStarted]
    Await.result(started, materializerTimeout.duration).materializer
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] def createAdditionalSystemMaterializer(defaultAttributes: Attributes): Materializer = {
    val started =
      (materializerGuardian ? MaterializerGuardian.StartMaterializer(Some(defaultAttributes)))
        .mapTo[MaterializerGuardian.MaterializerStarted]

    Await.result(started, materializerTimeout.duration).materializer
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def createAdditionalLegacySystemMaterializer(
      namePrefix: String,
      settings: ActorMaterializerSettings): Materializer = {
    val started =
      (materializerGuardian ? MaterializerGuardian.LegacyStartMaterializer(namePrefix, settings))
        .mapTo[MaterializerGuardian.MaterializerStarted]
    Await.result(started, materializerTimeout.duration).materializer
  }

  val materializer: Materializer = {
    val systemMaterializerFuture = systemMaterializerPromise.future
    systemMaterializerFuture.value match {
      case Some(tryMaterializer) =>
        tryMaterializer.get
      case None =>
        // block on async creation to make it effectively final
        Await.result(systemMaterializerFuture, materializerTimeout.duration)
    }
  }

}
