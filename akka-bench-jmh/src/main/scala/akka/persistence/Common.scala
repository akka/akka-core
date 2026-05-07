/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor.Actor

/** only as a "the best we could possibly get" baseline, does not persist anything */
class BaselineActor(respondAfter: Int) extends Actor {
  override def receive = {
    case n: Int => if (n == respondAfter) sender() ! n
  }
}

final case class Evt(i: Int)
