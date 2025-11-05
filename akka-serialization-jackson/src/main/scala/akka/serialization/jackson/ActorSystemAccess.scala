/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.serialization.Serialization

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait ActorSystemAccess {
  def currentSystem(): ExtendedActorSystem = {
    Serialization.currentTransportInformation.value match {
      case null =>
        throw new IllegalStateException(
          "Can't access current ActorSystem, Serialization.currentTransportInformation was not set.")
      case Serialization.Information(_, system) => system.asInstanceOf[ExtendedActorSystem]
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ActorSystemAccess extends ActorSystemAccess
