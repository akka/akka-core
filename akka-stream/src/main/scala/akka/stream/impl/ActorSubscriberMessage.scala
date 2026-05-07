/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import org.reactivestreams.Subscription

import akka.actor.DeadLetterSuppression
import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed abstract class ActorSubscriberMessage
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ActorSubscriberMessage {
  final case class OnNext(element: Any) extends ActorSubscriberMessage
  final case class OnError(cause: Throwable) extends ActorSubscriberMessage
  case object OnComplete extends ActorSubscriberMessage

  // OnSubscribe doesn't extend ActorSubscriberMessage by design, because `OnNext`, `OnError` and `OnComplete`
  // are used together, with the same `seal`, but not always `OnSubscribe`.
  final case class OnSubscribe(subscription: Subscription)
      extends DeadLetterSuppression
      with NoSerializationVerificationNeeded

}
