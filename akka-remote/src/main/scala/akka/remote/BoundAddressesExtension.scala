/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.remote.artery.ArteryTransport

/**
 * Extension provides access to bound addresses.
 */
object BoundAddressesExtension extends ExtensionId[BoundAddressesExtension] with ExtensionIdProvider {
  override def get(system: ActorSystem): BoundAddressesExtension = super.get(system)
  override def get(system: ClassicActorSystemProvider): BoundAddressesExtension = super.get(system)

  override def lookup = BoundAddressesExtension

  override def createExtension(system: ExtendedActorSystem): BoundAddressesExtension =
    new BoundAddressesExtension(system)
}

class BoundAddressesExtension(val system: ExtendedActorSystem) extends Extension {

  /**
   * Returns a mapping from a protocol to a set of bound addresses.
   */
  def boundAddresses: Map[String, Set[Address]] = system.provider.asInstanceOf[RemoteActorRefProvider].transport match {
    case artery: ArteryTransport => Map(ArteryTransport.ProtocolName -> Set(artery.bindAddress.address))
    case other                   => throw new IllegalStateException(s"Unexpected transport type: ${other.getClass}")
  }
}
