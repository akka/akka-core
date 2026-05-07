/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider

/**
 * Extension that holds a uid that is assigned as a random `Long`.
 *
 * The uid is intended to be used together with an [[akka.actor.Address]]
 * to be able to distinguish restarted actor system using the same host
 * and port.
 */
@deprecated("Use ExtendedActorSystem.uid instead.", "2.8.0")
object AddressUidExtension extends ExtensionId[AddressUidExtension] with ExtensionIdProvider {
  override def get(system: ActorSystem): AddressUidExtension = super.get(system)
  override def get(system: ClassicActorSystemProvider): AddressUidExtension = super.get(system)

  override def lookup = AddressUidExtension

  override def createExtension(system: ExtendedActorSystem): AddressUidExtension = new AddressUidExtension(system)

}

@deprecated("Use ExtendedActorSystem.uid instead.", "2.8.0")
class AddressUidExtension(val system: ExtendedActorSystem) extends Extension {

  val longAddressUid: Long =
    system.uid
}
