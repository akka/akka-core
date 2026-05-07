/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package internal
package adapter

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

import akka.{ actor => classic }
import akka.annotation.InternalApi
import akka.util.OptionVal

@InternalApi
private[akka] object ActorContextAdapter {

  private def toClassicImp[U](context: TypedActorContext[_]): classic.ActorContext =
    context match {
      case adapter: ActorContextAdapter[_] => adapter.classicActorContext
      case _ =>
        throw new UnsupportedOperationException(
          "Only adapted classic ActorContext permissible " +
          s"($context of class ${context.getClass.getName})")
    }

  def toClassic[U](context: scaladsl.ActorContext[_]): classic.ActorContext =
    toClassicImp(context)

  def toClassic[U](context: javadsl.ActorContext[_]): classic.ActorContext =
    toClassicImp(context)
}

/**
 * INTERNAL API. Wrapping an [[akka.actor.ActorContext]] as an [[TypedActorContext]].
 */
@InternalApi private[akka] final class ActorContextAdapter[T](adapter: ActorAdapter[T]) extends ActorContextImpl[T] {

  import ActorRefAdapter.toClassic
  private[akka] def classicActorContext = adapter.context

  private[akka] override def currentBehavior: Behavior[T] = adapter.currentBehavior

  // optimization to avoid adapter allocation unless used
  // self documented as thread safe, on purpose not volatile since
  // lazily created because creating adapter is idempotent
  private var _self: OptionVal[ActorRef[T]] = OptionVal.None
  override def self: ActorRef[T] = {
    if (_self.isEmpty) {
      _self = OptionVal.Some(ActorRefAdapter(classicActorContext.self))
    }
    _self.get
  }

  override def system = ActorSystemAdapter(classicActorContext.system)

  override def children: Iterable[ActorRef[Nothing]] = {
    checkCurrentActorThread()
    classicActorContext.children.map(ActorRefAdapter(_))
  }
  override def child(name: String): Option[ActorRef[Nothing]] = {
    checkCurrentActorThread()
    classicActorContext.child(name).map(ActorRefAdapter(_))
  }
  override def spawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty): ActorRef[U] = {
    checkCurrentActorThread()
    ActorRefFactoryAdapter.spawnAnonymous(classicActorContext, behavior, props, rethrowTypedFailure = true)
  }

  override def spawn[U](behavior: Behavior[U], name: String, props: Props = Props.empty): ActorRef[U] = {
    checkCurrentActorThread()
    ActorRefFactoryAdapter.spawn(classicActorContext, behavior, name, props, rethrowTypedFailure = true)
  }

  override def stop[U](child: ActorRef[U]): Unit = {
    checkCurrentActorThread()
    if (child.path.parent == self.path) { // only if a direct child
      toClassic(child) match {
        case f: akka.actor.FunctionRef =>
          val cell = classicActorContext.asInstanceOf[akka.actor.ActorCell]
          cell.removeFunctionRef(f)
        case c =>
          classicActorContext.child(child.path.name) match {
            case Some(`c`) =>
              classicActorContext.stop(c)
            case _ =>
            // child that was already stopped
          }
      }
    } else if (self == child) {
      throw new IllegalArgumentException(
        "Only direct children of an actor can be stopped through the actor context, " +
        s"but you tried to stop [$self] by passing its ActorRef to the `stop` method. " +
        "Stopping self has to be expressed as explicitly returning a Stop Behavior " +
        "with `Behaviors.stopped`.")
    } else {
      throw new IllegalArgumentException(
        "Only direct children of an actor can be stopped through the actor context, " +
        s"but [$child] is not a child of [$self]. Stopping other actors has to be expressed as " +
        "an explicit stop message that the actor accepts.")
    }
  }

  override def watch[U](other: ActorRef[U]): Unit = {
    checkCurrentActorThread()
    classicActorContext.watch(toClassic(other))
  }
  override def watchWith[U](other: ActorRef[U], msg: T): Unit = {
    checkCurrentActorThread()
    classicActorContext.watchWith(toClassic(other), msg)
  }
  override def unwatch[U](other: ActorRef[U]): Unit = {
    checkCurrentActorThread()
    classicActorContext.unwatch(toClassic(other))
  }
  var receiveTimeoutMsg: T = null.asInstanceOf[T]
  override def setReceiveTimeout(d: FiniteDuration, msg: T): Unit = {
    checkCurrentActorThread()
    receiveTimeoutMsg = msg
    classicActorContext.setReceiveTimeout(d)
  }
  override def cancelReceiveTimeout(): Unit = {
    checkCurrentActorThread()

    receiveTimeoutMsg = null.asInstanceOf[T]
    classicActorContext.setReceiveTimeout(Duration.Undefined)
  }
  override def executionContext: ExecutionContextExecutor = classicActorContext.dispatcher
  override def scheduleOnce[U](delay: FiniteDuration, target: ActorRef[U], msg: U): classic.Cancellable = {
    classicActorContext.system.scheduler.scheduleOnce(delay, toClassic(target), msg)(classicActorContext.dispatcher)
  }
  override private[akka] def internalSpawnMessageAdapter[U](f: U => T, _name: String): ActorRef[U] = {
    val cell = classicActorContext.asInstanceOf[akka.actor.ActorCell]
    // apply the function inside the actor by wrapping the msg and f, handled by ActorAdapter
    val ref =
      cell.addFunctionRef((_, msg) => classicActorContext.self ! AdaptMessage[U, T](msg.asInstanceOf[U], f), _name)
    ActorRefAdapter[U](ref)
  }

  /**
   * Made accessible to allow stash to deal with unhandled messages as though they were interpreted by
   * the adapter itself, even though the unstashing occurs inside the behavior stack.
   */
  private[akka] override def onUnhandled(msg: T): Unit = adapter.unhandled(msg)
}
