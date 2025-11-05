/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import scala.util.control.NonFatal

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.actor.Terminated

/**
 * Main class to start an [[akka.actor.ActorSystem]] with one
 * top level application supervisor actor. It will shutdown
 * the actor system when the top level actor is terminated.
 */
@deprecated("Implement your own main class instead, from which you start the ActorSystem and actors.", "2.6.0")
object Main {

  /**
   * @param args one argument: the class of the application supervisor actor
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("you need to provide exactly one argument: the class of the application supervisor actor")
    } else {
      val system = ActorSystem("Main")
      try {
        val appClass = system.asInstanceOf[ExtendedActorSystem].dynamicAccess.getClassFor[Actor](args(0)).get
        val app = system.actorOf(Props(appClass), "app")
        system.actorOf(Props(classOf[Terminator], app), "app-terminator")
      } catch {
        case NonFatal(e) => system.terminate(); throw e
      }
    }
  }

  class Terminator(app: ActorRef) extends Actor with ActorLogging {
    context.watch(app)
    def receive = {
      case Terminated(_) =>
        log.info("application supervisor has terminated, shutting down")
        context.system.terminate()
    }
  }

}
