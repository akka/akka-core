/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adapter

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi

/**
 * Internal API
 *
 * To not create a new adapter for every `toTyped` call we create one instance and keep in an extension
 */
@InternalApi private[akka] class AdapterExtension(sys: akka.actor.ActorSystem) extends akka.actor.Extension {
  val adapter = ActorSystemAdapter(sys)
}

/**
 * Internal API
 */
@InternalApi object AdapterExtension extends akka.actor.ExtensionId[AdapterExtension] {
  def createExtension(sys: ExtendedActorSystem): AdapterExtension = new AdapterExtension(sys)
}
