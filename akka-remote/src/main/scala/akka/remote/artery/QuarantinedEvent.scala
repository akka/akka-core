/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.remote.UniqueAddress

final case class QuarantinedEvent(uniqueAddress: UniqueAddress) {

  override val toString: String =
    s"QuarantinedEvent: Association to [${uniqueAddress.address}] having UID [${uniqueAddress.uid}] is" +
    "irrecoverably failed. UID is now quarantined and all messages to this UID will be delivered to dead letters. " +
    "Remote ActorSystem must be restarted to recover from this situation."
}

final case class GracefulShutdownQuarantinedEvent(uniqueAddress: UniqueAddress, reason: String) {
  override val toString: String =
    s"GracefulShutdownQuarantinedEvent: Association to [${uniqueAddress.address}] having UID [${uniqueAddress.uid}] " +
    s"has been stopped. All messages to this UID will be delivered to dead letters. Reason: $reason"
}

final case class ThisActorSystemQuarantinedEvent(localAddress: UniqueAddress, remoteAddress: UniqueAddress) {
  override val toString: String =
    s"ThisActorSystemQuarantinedEvent: The remote system [$remoteAddress] has quarantined this system [$localAddress]."
}
