/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed.fromclassic

// #hello-world-actor
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props

// #hello-world-actor

object ClassicSample {

  //#hello-world-actor
  object HelloWorld {
    final case class Greet(whom: String)
    final case class Greeted(whom: String)

    def props(): Props =
      Props(new HelloWorld)
  }

  class HelloWorld extends Actor with ActorLogging {
    import HelloWorld._

    override def receive: Receive = {
      case Greet(whom) =>
        log.info("Hello {}!", whom)
        sender() ! Greeted(whom)
    }
  }
  //#hello-world-actor

}
