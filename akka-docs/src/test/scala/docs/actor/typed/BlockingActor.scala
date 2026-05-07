/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor.typed

// #blocking-in-actor
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object BlockingActor {
  def apply(): Behavior[Int] =
    Behaviors.receiveMessage { i =>
      // DO NOT DO THIS HERE: this is an example of incorrect code,
      // better alternatives are described further on.

      //block for 5 seconds, representing blocking I/O, etc
      Thread.sleep(5000)
      println(s"Blocking operation finished: $i")
      Behaviors.same
    }
}
// #blocking-in-actor
