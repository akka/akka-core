/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor._

/**
 * INTERNAL API
 */
private[akka] final case class RARP(provider: RemoteActorRefProvider) extends Extension {
  def configureDispatcher(props: Props): Props = provider.remoteSettings.configureDispatcher(props)
}

/**
 * INTERNAL API
 */
private[akka] object RARP extends ExtensionId[RARP] with ExtensionIdProvider {

  override def lookup = RARP

  override def createExtension(system: ExtendedActorSystem) = RARP(system.provider.asInstanceOf[RemoteActorRefProvider])
}

/**
 * INTERNAL API
 * Messages marked with this trait will be sent before other messages when buffering is active.
 * This means that these messages don't obey normal message ordering.
 * It is used for failure detector heartbeat messages.
 */
private[akka] trait PriorityMessage

/**
 * Failure detector heartbeat messages are marked with this trait.
 */
private[akka] trait HeartbeatMessage extends PriorityMessage
