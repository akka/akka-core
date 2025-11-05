/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.annotation.nowarn

import akka.ConfigurationException
import akka.actor.ActorPath
import akka.actor.ActorSystemImpl
import akka.actor.Cell
import akka.actor.InternalActorRef
import akka.actor.Props
import akka.actor.RepointableActorRef
import akka.actor.UnstartedCell
import akka.dispatch.BalancingDispatcher
import akka.dispatch.MailboxType
import akka.dispatch.MessageDispatcher

/**
 * INTERNAL API
 *
 * A RoutedActorRef is an ActorRef that has a set of connected ActorRef and it uses a Router to
 * send a message to one (or more) of these actors.
 */
@nowarn("msg=deprecated")
private[akka] class RoutedActorRef(
    _system: ActorSystemImpl,
    _routerProps: Props,
    _routerDispatcher: MessageDispatcher,
    _routerMailbox: MailboxType,
    _routeeProps: Props,
    _supervisor: InternalActorRef,
    _path: ActorPath)
    extends RepointableActorRef(_system, _routerProps, _routerDispatcher, _routerMailbox, _supervisor, _path) {

  // verify that a BalancingDispatcher is not used with a Router
  if (_routerProps.routerConfig != NoRouter && _routerDispatcher.isInstanceOf[BalancingDispatcher]) {
    throw new ConfigurationException(
      "Configuration for " + this +
      " is invalid - you can not use a 'BalancingDispatcher' as a Router's dispatcher, you can however use it for the routees.")
  } else _routerProps.routerConfig.verifyConfig(_path)

  override def newCell(old: UnstartedCell): Cell = {
    val cell = props.routerConfig match {
      case pool: Pool if pool.resizer.isDefined =>
        new ResizablePoolCell(system, this, props, dispatcher, _routeeProps, supervisor, pool)
      case _ =>
        new RoutedActorCell(system, this, props, dispatcher, _routeeProps, supervisor)
    }
    cell.init(sendSupervise = false, mailboxType)
  }

}
