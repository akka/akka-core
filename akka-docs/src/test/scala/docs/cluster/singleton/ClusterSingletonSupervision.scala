/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.cluster.singleton

//#singleton-supervisor-actor
import akka.actor.{ Actor, Props, SupervisorStrategy }
class SupervisorActor(childProps: Props, override val supervisorStrategy: SupervisorStrategy) extends Actor {
  val child = context.actorOf(childProps, "supervised-child")

  def receive = {
    case msg => child.forward(msg)
  }
}
//#singleton-supervisor-actor

import akka.actor.Actor
abstract class ClusterSingletonSupervision extends Actor {
  import akka.actor.{ ActorRef, Props, SupervisorStrategy }
  def createSingleton(name: String, props: Props, supervisorStrategy: SupervisorStrategy): ActorRef = {
    //#singleton-supervisor-actor-usage
    import akka.actor.{ PoisonPill, Props }
    import akka.cluster.singleton.{ ClusterSingletonManager, ClusterSingletonManagerSettings }
    context.system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(classOf[SupervisorActor], props, supervisorStrategy),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(context.system)),
      name = name)
    //#singleton-supervisor-actor-usage
  }
}
