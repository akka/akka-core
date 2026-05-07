/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.receptionist

import akka.actor.typed.ActorRef
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[receptionist] object Platform {
  type Service[K <: AbstractServiceKey] = ActorRef[K#Protocol]
  type Subscriber[K <: AbstractServiceKey] = ActorRef[ReceptionistMessages.Listing[K#Protocol]]
}
