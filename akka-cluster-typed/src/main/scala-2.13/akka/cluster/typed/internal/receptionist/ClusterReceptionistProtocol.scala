/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal.receptionist

import akka.actor.typed.ActorRef
import akka.actor.typed.internal.receptionist.{ AbstractServiceKey, ReceptionistMessages }
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[receptionist] object ClusterReceptionistProtocol {
  type SubscriptionsKV[K <: AbstractServiceKey] = ActorRef[ReceptionistMessages.Listing[K#Protocol]]
}
