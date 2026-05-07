/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor.typed

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

// #print-actor
object PrintActor {
  def apply(): Behavior[Integer] =
    Behaviors.receiveMessage { i =>
      println(s"PrintActor: $i")
      Behaviors.same
    }
}
// #print-actor
