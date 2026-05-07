/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

class SharedMutableStateDocSpec {

  //#mutable-state
  import akka.actor.{ Actor, ActorRef }
  import akka.pattern.ask
  import akka.util.Timeout
  import scala.concurrent.Future
  import scala.concurrent.duration._
  import scala.collection.mutable

  case class Message(msg: String)

  class EchoActor extends Actor {
    def receive = {
      case msg => sender() ! msg
    }
  }

  class CleanUpActor extends Actor {
    def receive = {
      case set: mutable.Set[_] => set.clear()
    }
  }

  class MyActor(echoActor: ActorRef, cleanUpActor: ActorRef) extends Actor {
    var state = ""
    val mySet = mutable.Set[String]()

    def expensiveCalculation(actorRef: ActorRef): String = {
      // this is a very costly operation
      "Meaning of life is 42"
    }

    def expensiveCalculation(): String = {
      // this is a very costly operation
      "Meaning of life is 42"
    }

    def receive = {
      case _ =>
        implicit val ec = context.dispatcher
        implicit val timeout: Timeout = 5.seconds // needed for `?` below

        // Example of incorrect approach
        // Very bad: shared mutable state will cause your
        // application to break in weird ways
        Future { state = "This will race" }
        ((echoActor ? Message("With this other one")).mapTo[Message]).foreach { received =>
          state = received.msg
        }

        // Very bad: shared mutable object allows
        // the other actor to mutate your own state,
        // or worse, you might get weird race conditions
        cleanUpActor ! mySet

        // Very bad: "sender" changes for every message,
        // shared mutable state bug
        Future { expensiveCalculation(sender()) }

        // Example of correct approach
        // Completely safe: "self" is OK to close over
        // and it's an ActorRef, which is thread-safe
        Future { expensiveCalculation() }.foreach { self ! _ }

        // Completely safe: we close over a fixed value
        // and it's an ActorRef, which is thread-safe
        val currentSender = sender()
        Future { expensiveCalculation(currentSender) }
    }
  }
  //#mutable-state
}
