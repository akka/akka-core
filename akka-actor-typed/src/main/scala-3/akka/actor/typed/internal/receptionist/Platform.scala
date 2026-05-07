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
  type Aux[P] = AbstractServiceKey { type Protocol = P }

  type Service[K <: Aux[_]] = K match {
    case Aux[t] => ActorRef[t]
  }

  type Subscriber[K <: Aux[_]] = K match {
    case Aux[t] => ActorRef[ReceptionistMessages.Listing[t]]
  }
}
