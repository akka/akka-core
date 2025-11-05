/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.actor.ActorRef

/**
 * Used as failure exception by an `ask` operator if the target actor terminates.
 * See `Flow.ask` and `Flow.watch`.
 */
final class WatchedActorTerminatedException(val watchingStageName: String, val ref: ActorRef)
    extends RuntimeException(s"Actor watched by [$watchingStageName] has terminated! Was: $ref")
