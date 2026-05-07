/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.actor.Actor
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] abstract class ExposedPublisherReceive(activeReceive: Actor.Receive, unhandled: Any => Unit)
    extends Actor.Receive {
  private var stash = List.empty[Any]

  def isDefinedAt(o: Any): Boolean = true

  def apply(o: Any): Unit = o match {
    case ep: ExposedPublisher =>
      receiveExposedPublisher(ep)
      if (stash.nonEmpty) {
        // we don't use sender() so this is alright
        stash.reverse.foreach { msg =>
          activeReceive.applyOrElse(msg, unhandled)
        }
      }
    case other =>
      stash ::= other
  }

  def receiveExposedPublisher(ep: ExposedPublisher): Unit
}
