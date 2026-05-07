/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.routing

import java.util.function

import akka.actor.Dropped
import akka.actor.typed._
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, StashBuffer }
import akka.annotation.InternalApi

/**
 * Provides builder style configuration options for group routers while still being a behavior that can be spawned
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final case class GroupRouterBuilder[T] private[akka] (
    key: ServiceKey[T],
    preferLocalRoutees: Boolean = false,
    logicFactory: ActorSystem[_] => RoutingLogic[T] = (_: ActorSystem[_]) => new RoutingLogics.RandomLogic[T]())
    extends javadsl.GroupRouter[T]
    with scaladsl.GroupRouter[T] {

  // deferred creation of the actual router
  def apply(ctx: TypedActorContext[T]): Behavior[T] =
    new InitialGroupRouterImpl[T](ctx.asScala, key, preferLocalRoutees, logicFactory(ctx.asScala.system))

  def withRandomRouting(): GroupRouterBuilder[T] = withRandomRouting(false)

  def withRandomRouting(preferLocalRoutees: Boolean): GroupRouterBuilder[T] =
    copy(preferLocalRoutees = preferLocalRoutees, logicFactory = _ => new RoutingLogics.RandomLogic[T]())

  def withRoundRobinRouting(): GroupRouterBuilder[T] = withRoundRobinRouting(false)

  def withRoundRobinRouting(preferLocalRoutees: Boolean): GroupRouterBuilder[T] =
    copy(preferLocalRoutees = preferLocalRoutees, logicFactory = _ => new RoutingLogics.RoundRobinLogic[T])

  def withConsistentHashingRouting(
      virtualNodesFactor: Int,
      mapping: function.Function[T, String]): GroupRouterBuilder[T] =
    withConsistentHashingRouting(virtualNodesFactor, mapping.apply(_))

  def withConsistentHashingRouting(virtualNodesFactor: Int, mapping: T => String): GroupRouterBuilder[T] = {
    copy(
      preferLocalRoutees = false,
      logicFactory = system => new RoutingLogics.ConsistentHashingLogic[T](virtualNodesFactor, mapping, system.address))
  }
}

/**
 * INTERNAL API
 *
 * Starting behavior for a group router before it got a first listing back from the receptionist
 */
@InternalApi
private final class InitialGroupRouterImpl[T](
    ctx: ActorContext[T],
    serviceKey: ServiceKey[T],
    preferLocalRoutees: Boolean,
    routingLogic: RoutingLogic[T])
    extends AbstractBehavior[T](ctx) {

  // casting trix to avoid having to wrap incoming messages - note that this will cause problems if intercepting
  // messages to a router
  context.system.receptionist ! Receptionist.Subscribe(
    serviceKey,
    context.self.unsafeUpcast[Any].narrow[Receptionist.Listing])

  private val stash = StashBuffer[T](context, capacity = 10000)

  def onMessage(msg: T): Behavior[T] = msg match {
    case serviceKey.Listing(allRoutees) =>
      val update = GroupRouterHelper.routeesToUpdate(allRoutees, preferLocalRoutees)
      // we don't need to watch, because receptionist already does that
      routingLogic.routeesUpdated(update)
      val activeGroupRouter =
        new GroupRouterImpl[T](context, serviceKey, preferLocalRoutees, routingLogic, update.isEmpty)
      stash.unstashAll(activeGroupRouter)
    case msg: T @unchecked =>
      import akka.actor.typed.scaladsl.adapter._
      if (!stash.isFull) stash.stash(msg)
      else
        context.system.eventStream ! EventStream.Publish(Dropped(
          msg,
          s"Stash is full in group router for [$serviceKey]",
          context.self.toClassic)) // don't fail on full stash
      this
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[routing] object GroupRouterHelper {
  def routeesToUpdate[T](allRoutees: Set[ActorRef[T]], preferLocalRoutees: Boolean): Set[ActorRef[T]] = {
    if (preferLocalRoutees) {
      val localRoutees = allRoutees.filter(_.path.address.hasLocalScope)
      if (localRoutees.nonEmpty) localRoutees else allRoutees
    } else allRoutees
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class GroupRouterImpl[T](
    ctx: ActorContext[T],
    serviceKey: ServiceKey[T],
    preferLocalRoutees: Boolean,
    routingLogic: RoutingLogic[T],
    routeesInitiallyEmpty: Boolean)
    extends AbstractBehavior[T](ctx) {

  private var routeesEmpty = routeesInitiallyEmpty

  def onMessage(msg: T): Behavior[T] = msg match {
    case l @ serviceKey.Listing(allRoutees) =>
      context.log.debug("Update from receptionist: [{}]", l)
      val update = GroupRouterHelper.routeesToUpdate(allRoutees, preferLocalRoutees)

      val routees =
        if (update.nonEmpty) update
        else
          // empty listing in a cluster context can mean all nodes with registered services
          // are unreachable, in that case trying the unreachable ones is better than dropping messages
          l.allServiceInstances(serviceKey)
      routeesEmpty = routees.isEmpty
      routingLogic.routeesUpdated(routees)
      this
    case msg: T @unchecked =>
      import akka.actor.typed.scaladsl.adapter._
      if (!routeesEmpty) routingLogic.selectRoutee(msg) ! msg
      else
        context.system.eventStream ! EventStream.Publish(
          Dropped(msg, s"No routees in group router for [$serviceKey]", context.self.toClassic))
      this
  }
}
