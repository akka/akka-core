/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package scaladsl

import akka.actor.ExtendedActorSystem
import akka.actor.typed.internal.adapter.{ PropsAdapter => _, _ }
import akka.annotation.InternalApi

/**
 * Adapters between typed and classic actors and actor systems.
 * The underlying `ActorSystem` is the classic [[akka.actor.ActorSystem]]
 * which runs Akka Typed [[akka.actor.typed.Behavior]] on an emulation layer. In this
 * system typed and classic actors can coexist.
 *
 * Use these adapters with `import akka.actor.typed.scaladsl.adapter._`.
 *
 * Implicit extension methods are added to classic and typed `ActorSystem`,
 * `ActorContext`. Such methods make it possible to create typed child actor
 * from classic parent actor, and the opposite classic child from typed parent.
 * `watch` is also supported in both directions.
 *
 * There is an implicit conversion from classic [[akka.actor.ActorRef]] to
 * typed [[akka.actor.typed.ActorRef]].
 *
 * There are also converters (`toTyped`, `toClassic`) from typed
 * [[akka.actor.typed.ActorRef]] to classic [[akka.actor.ActorRef]], and between classic
 * [[akka.actor.ActorSystem]] and typed [[akka.actor.typed.ActorSystem]].
 */
package object adapter {

  import language.implicitConversions

  /**
   * Extension methods added to [[akka.actor.ActorSystem]].
   */
  implicit class ClassicActorSystemOps(val sys: akka.actor.ActorSystem) extends AnyVal {

    /**
     *  Spawn the given behavior as a child of the user actor in a classic ActorSystem.
     *
     *  Typed actors default supervision strategy is to stop. Can be overridden with
     *  `Behaviors.supervise`.
     */
    def spawnAnonymous[T](behavior: Behavior[T], props: Props = Props.empty): ActorRef[T] = {
      ActorRefFactoryAdapter.spawnAnonymous(
        sys,
        Behaviors.supervise(behavior).onFailure(SupervisorStrategy.stop),
        props,
        rethrowTypedFailure = false)
    }

    /**
     *  Spawn the given behavior as a child of the user actor in a classic ActorSystem.
     *
     *  Typed actors default supervision strategy is to stop. Can be overridden with
     *  `Behaviors.supervise`.
     */
    def spawn[T](behavior: Behavior[T], name: String, props: Props = Props.empty): ActorRef[T] = {
      ActorRefFactoryAdapter.spawn(
        sys,
        Behaviors.supervise(behavior).onFailure(SupervisorStrategy.stop),
        name,
        props,
        rethrowTypedFailure = false)
    }

    def toTyped: ActorSystem[Nothing] = AdapterExtension(sys).adapter
  }

  /**
   * Extension methods added to [[akka.actor.typed.ActorSystem]].
   */
  implicit class TypedActorSystemOps(val sys: ActorSystem[_]) extends AnyVal {
    def toClassic: akka.actor.ActorSystem = sys.classicSystem

    /**
     * INTERNAL API
     */
    @InternalApi private[akka] def internalSystemActorOf[U](
        behavior: Behavior[U],
        name: String,
        props: Props): ActorRef[U] = {
      toClassic.asInstanceOf[ExtendedActorSystem].systemActorOf(PropsAdapter(behavior, props), name)
    }
  }

  /**
   * Extension methods added to [[akka.actor.ActorContext]].
   */
  implicit class ClassicActorContextOps(val ctx: akka.actor.ActorContext) extends AnyVal {

    /**
     *  Spawn the given behavior as a child of the user actor in a classic ActorContext.
     *
     *  Typed actors default supervision strategy is to stop. Can be overridden with
     *  `Behaviors.supervise`.
     */
    def spawnAnonymous[T](behavior: Behavior[T], props: Props = Props.empty): ActorRef[T] =
      ActorRefFactoryAdapter.spawnAnonymous(
        ctx,
        Behaviors.supervise(behavior).onFailure(SupervisorStrategy.stop),
        props,
        rethrowTypedFailure = false)

    /**
     *  Spawn the given behavior as a child of the user actor in a classic ActorContext.
     *
     *  Typed actors default supervision strategy is to stop. Can be overridden with
     *  `Behaviors.supervise`.
     */
    def spawn[T](behavior: Behavior[T], name: String, props: Props = Props.empty): ActorRef[T] =
      ActorRefFactoryAdapter.spawn(
        ctx,
        Behaviors.supervise(behavior).onFailure(SupervisorStrategy.stop),
        name,
        props,
        rethrowTypedFailure = false)

    def watch[U](other: ActorRef[U]): Unit = ctx.watch(ActorRefAdapter.toClassic(other))
    def unwatch[U](other: ActorRef[U]): Unit = ctx.unwatch(ActorRefAdapter.toClassic(other))

    def stop(child: ActorRef[_]): Unit =
      ctx.stop(ActorRefAdapter.toClassic(child))
  }

  /**
   * Extension methods added to [[akka.actor.typed.scaladsl.ActorContext]].
   */
  implicit class TypedActorContextOps(val ctx: scaladsl.ActorContext[_]) extends AnyVal {
    def actorOf(props: akka.actor.Props): akka.actor.ActorRef =
      ActorContextAdapter.toClassic(ctx).actorOf(props)

    def actorOf(props: akka.actor.Props, name: String): akka.actor.ActorRef =
      ActorContextAdapter.toClassic(ctx).actorOf(props, name)

    def toClassic: akka.actor.ActorContext = ActorContextAdapter.toClassic(ctx)

    // watch, unwatch and stop not needed here because of the implicit ActorRef conversion
  }

  /**
   * Extension methods added to [[akka.actor.typed.ActorRef]].
   */
  implicit class TypedActorRefOps(val ref: ActorRef[_]) extends AnyVal {
    def toClassic: akka.actor.ActorRef = ActorRefAdapter.toClassic(ref)
  }

  /**
   * Extension methods added to [[akka.actor.ActorRef]].
   */
  implicit class ClassicActorRefOps(val ref: akka.actor.ActorRef) extends AnyVal {

    /**
     * Adapt the classic `ActorRef` to `akka.actor.typed.ActorRef[T]`. There is also an
     * automatic implicit conversion for this, but this more explicit variant might
     * sometimes be preferred.
     */
    def toTyped[T]: ActorRef[T] = ActorRefAdapter(ref)
  }

  /**
   * Implicit conversion from classic [[akka.actor.ActorRef]] to [[akka.actor.typed.ActorRef]].
   */
  implicit def actorRefAdapter[T](ref: akka.actor.ActorRef): ActorRef[T] = ActorRefAdapter(ref)

  /**
   * Extension methods added to [[akka.actor.typed.Scheduler]].
   */
  implicit class TypedSchedulerOps(val scheduler: Scheduler) extends AnyVal {
    def toClassic: akka.actor.Scheduler = SchedulerAdapter.toClassic(scheduler)
  }

  /**
   * Extension methods added to [[akka.actor.Scheduler]].
   */
  implicit class ClassicSchedulerOps(val scheduler: akka.actor.Scheduler) extends AnyVal {

    /**
     * Adapt the classic `Scheduler` to `akka.actor.typed.Scheduler`.
     */
    def toTyped: Scheduler = new SchedulerAdapter(scheduler)
  }

}
