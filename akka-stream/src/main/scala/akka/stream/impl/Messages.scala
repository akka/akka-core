/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.actor.{ DeadLetterSuppression, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object SubscribePending
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class RequestMore[T](subscription: ActorSubscription[T], demand: Long)
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class Cancel[T](subscription: ActorSubscription[T])
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class ExposedPublisher(publisher: ActorPublisher[Any])
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded
