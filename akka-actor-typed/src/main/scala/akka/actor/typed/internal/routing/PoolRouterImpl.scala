/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.routing

import java.util.function
import java.util.function.Predicate

import akka.actor.typed._
import akka.actor.typed.javadsl.PoolRouter
import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, Behaviors }
import akka.annotation.InternalApi
import akka.util.ConstantFun

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class PoolRouterBuilder[T](
    poolSize: Int,
    behavior: Behavior[T],
    logicFactory: ActorSystem[_] => RoutingLogic[T] = (_: ActorSystem[_]) => new RoutingLogics.RoundRobinLogic[T],
    broadcastPredicate: T => Boolean = ConstantFun.anyToFalse,
    routeeProps: Props = Props.empty)
    extends javadsl.PoolRouter[T]
    with scaladsl.PoolRouter[T] {
  if (poolSize < 1) throw new IllegalArgumentException(s"pool size must be positive, was $poolSize")

  // deferred creation of the actual router
  def apply(ctx: TypedActorContext[T]): Behavior[T] =
    new PoolRouterImpl[T](
      ctx.asScala,
      poolSize,
      behavior,
      logicFactory(ctx.asScala.system),
      broadcastPredicate,
      routeeProps)

  def withRandomRouting(): PoolRouterBuilder[T] = copy(logicFactory = _ => new RoutingLogics.RandomLogic[T]())

  def withRoundRobinRouting(): PoolRouterBuilder[T] = copy(logicFactory = _ => new RoutingLogics.RoundRobinLogic[T])

  def withConsistentHashingRouting(virtualNodesFactor: Int, mapping: function.Function[T, String]): PoolRouter[T] =
    withConsistentHashingRouting(virtualNodesFactor, mapping.apply(_))

  def withConsistentHashingRouting(virtualNodesFactor: Int, mapping: T => String): PoolRouterBuilder[T] = {
    copy(
      logicFactory = system => new RoutingLogics.ConsistentHashingLogic[T](virtualNodesFactor, mapping, system.address))
  }

  def withPoolSize(poolSize: Int): PoolRouterBuilder[T] = copy(poolSize = poolSize)

  def withRouteeProps(routeeProps: Props): PoolRouterBuilder[T] = copy(routeeProps = routeeProps)

  override def withBroadcastPredicate(pred: Predicate[T]): PoolRouter[T] =
    copy(broadcastPredicate = value => pred.test(value))

  override def withBroadcastPredicate(pred: T => Boolean): scaladsl.PoolRouter[T] = copy(broadcastPredicate = pred)
}

/**
 * INTERNAL API
 */
@InternalApi
private final class PoolRouterImpl[T](
    ctx: ActorContext[T],
    poolSize: Int,
    behavior: Behavior[T],
    logic: RoutingLogic[T],
    broadcastPredicate: T => Boolean,
    routeeProps: Props)
    extends AbstractBehavior[T](ctx) {

  (1 to poolSize).foreach { _ =>
    val child = context.spawnAnonymous(behavior, routeeProps)
    context.watch(child)
    child
  }
  onRouteesChanged()

  private def onRouteesChanged(): Unit = {
    val children = context.children.toSet.asInstanceOf[Set[ActorRef[T]]]
    logic.routeesUpdated(children)
  }

  def onMessage(msg: T): Behavior[T] = {
    if ((broadcastPredicate ne ConstantFun.anyToFalse) && broadcastPredicate(msg)) {
      ctx.children.foreach(_.unsafeUpcast[Any] ! msg)
    } else {
      logic.selectRoutee(msg) ! msg
    }
    this
  }

  override def onSignal: PartialFunction[Signal, Behavior[T]] = {
    case Terminated(child) =>
      // Note that if several children are stopping concurrently children may already be empty
      // for the `Terminated` we receive for the first child. This means it is not certain that
      // there will be a log entry per child in those cases (it does not make sense to keep the
      // pool alive just to get the logging right when there are no routees available)
      if (context.children.nonEmpty) {
        context.log.debug("Pool child stopped [{}]", child.path)
        onRouteesChanged()
        this
      } else {
        context.log.info("Last pool child stopped, stopping pool [{}]", context.self.path)
        Behaviors.stopped
      }
  }

}
