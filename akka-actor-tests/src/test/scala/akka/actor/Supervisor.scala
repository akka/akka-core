/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

/**
 * For testing Supervisor behavior, normally you don't supply the strategy
 * from the outside like this.
 */
class Supervisor(override val supervisorStrategy: SupervisorStrategy) extends Actor {

  def receive = {
    case x: Props => sender() ! context.actorOf(x)
  }
  // need to override the default of stopping all children upon restart, tests rely on keeping them around
  override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {}
}
