/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.nowarn
import scala.concurrent.Promise

import akka.actor.Actor
import akka.actor.Props
import akka.annotation.InternalApi
import akka.stream.ActorMaterializerSettings
import akka.stream.Materializer

/**
 * INTERNAL API
 *
 * The materializer guardian is parent to all materializers created on the `system` level including the default
 * system wide materializer. Eagerly started by the SystemMaterializer extension on system startup.
 */
@InternalApi
private[akka] object MaterializerGuardian {

  case object StartMaterializer
  final case class MaterializerStarted(materializer: Materializer)

  // this is available to keep backwards compatibility with ActorMaterializer and should
  // be removed together with ActorMaterialixer in Akka 2.7
  final case class LegacyStartMaterializer(namePrefix: String, settings: ActorMaterializerSettings)

  def props(systemMaterializer: Promise[Materializer], materializerSettings: ActorMaterializerSettings) =
    Props(new MaterializerGuardian(systemMaterializer, materializerSettings))
}

/**
 * INTERNAL API
 */
@nowarn("msg=deprecated")
@InternalApi
private[akka] final class MaterializerGuardian(
    systemMaterializerPromise: Promise[Materializer],
    materializerSettings: ActorMaterializerSettings)
    extends Actor {
  import MaterializerGuardian._

  private val defaultAttributes = materializerSettings.toAttributes
  private val defaultNamePrefix = "flow"

  private val systemMaterializer = startMaterializer(defaultNamePrefix, None)
  systemMaterializerPromise.success(systemMaterializer)

  override def receive: Receive = {
    case StartMaterializer =>
      sender() ! MaterializerStarted(startMaterializer(defaultNamePrefix, None))
    case LegacyStartMaterializer(namePrefix, settings) =>
      sender() ! MaterializerStarted(startMaterializer(namePrefix, Some(settings)))
  }

  private def startMaterializer(namePrefix: String, settings: Option[ActorMaterializerSettings]) = {
    val attributes = settings match {
      case None                         => defaultAttributes
      case Some(`materializerSettings`) => defaultAttributes
      case Some(settings)               => settings.toAttributes
    }

    PhasedFusingActorMaterializer(context, namePrefix, settings.getOrElse(materializerSettings), attributes)
  }
}
