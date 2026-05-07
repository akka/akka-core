/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import java.util.function.{ Function => JFunction }
import java.util.function.Predicate

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.util.control.NonFatal

import akka.actor.DeadLetter
import akka.actor.typed.Behavior
import akka.actor.typed.Signal
import akka.actor.typed.TypedActorContext
import akka.actor.typed.javadsl
import akka.actor.typed.scaladsl
import akka.actor.typed.scaladsl.ActorContext
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.japi.function.Procedure
import akka.util.ConstantFun
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[akka] object StashBufferImpl {
  private[akka] final class Node[T](var next: Node[T], val message: T) {
    def apply(f: T => Unit): Unit = f(message)
  }

  def apply[T](ctx: ActorContext[T], capacity: Int): StashBufferImpl[T] =
    new StashBufferImpl(ctx, capacity, null, null)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class StashBufferImpl[T] private (
    ctx: ActorContext[T],
    val capacity: Int,
    private var _first: StashBufferImpl.Node[T],
    private var _last: StashBufferImpl.Node[T])
    extends javadsl.StashBuffer[T]
    with scaladsl.StashBuffer[T] {

  import StashBufferImpl.Node

  private var _size: Int = if (_first eq null) 0 else 1

  private var currentBehaviorWhenUnstashInProgress: OptionVal[Behavior[T]] = OptionVal.None

  override def isEmpty: Boolean = _first eq null

  override def nonEmpty: Boolean = !isEmpty

  override def size: Int = _size

  override def isFull: Boolean = _size == capacity

  override def stash(message: T): StashBufferImpl[T] = {
    if (message == null) throw new NullPointerException
    if (isFull)
      throw new javadsl.StashOverflowException(
        s"Couldn't add [${message.getClass.getName}] " +
        s"because stash with capacity [$capacity] is full")

    val node = createNode(message, ctx)
    if (isEmpty) {
      _first = node
      _last = node
    } else {
      _last.next = node
      _last = node
    }

    _size += 1
    this
  }

  override def clear(): Unit = {
    _first = null
    _last = null
    _size = 0
    stashCleared(ctx)
  }

  @InternalStableApi
  private def createNode(message: T, @nowarn("msg=never used") ctx: scaladsl.ActorContext[T]): Node[T] = {
    new Node(null, message)
  }

  @InternalStableApi
  private def dropHeadForUnstash(): Node[T] = {
    val message = rawHead
    _first = _first.next
    _size -= 1
    if (isEmpty)
      _last = null

    message
  }

  @InternalStableApi
  private def interpretUnstashedMessage(
      behavior: Behavior[T],
      ctx: TypedActorContext[T],
      wrappedMessage: T,
      @nowarn("msg=never used") node: Node[T]): Behavior[T] = {
    Behavior.interpretMessage(behavior, ctx, wrappedMessage)
  }

  private def rawHead: Node[T] =
    if (nonEmpty) _first
    else throw new NoSuchElementException("head of empty buffer")

  override def head: T =
    if (nonEmpty) _first.message
    else throw new NoSuchElementException("head of empty buffer")

  override def foreach(f: T => Unit): Unit = {
    var node = _first
    while (node ne null) {
      node(f)
      node = node.next
    }
  }

  override def forEach(f: Procedure[T]): Unit = foreach(f.apply)

  override def contains[U >: T](message: U): Boolean =
    exists(_ == message)

  override def exists(predicate: T => Boolean): Boolean = {
    var hasElement = false
    var node = _first
    while (node != null && !hasElement) {
      hasElement = predicate(node.message)
      node = node.next
    }
    hasElement
  }

  override def anyMatch(predicate: Predicate[T]): Boolean = exists(predicate.test)

  override def unstashAll(behavior: Behavior[T]): Behavior[T] = {
    val behav = unstash(behavior, size, ConstantFun.scalaIdentityFunction[T])
    stashCleared(ctx)
    behav
  }

  override def unstash(behavior: Behavior[T], numberOfMessages: Int, wrap: T => T): Behavior[T] = {
    if (isEmpty)
      behavior // optimization
    else {
      // currentBehaviorWhenUnstashInProgress is needed to keep track of current Behavior for Behaviors.same
      // when unstash is called when a previous unstash is already in progress (in same call stack)
      val unstashAlreadyInProgress = currentBehaviorWhenUnstashInProgress.isDefined
      try {
        val iter = new Iterator[Node[T]] {
          override def hasNext: Boolean = StashBufferImpl.this.nonEmpty

          override def next(): Node[T] = {
            val next = StashBufferImpl.this.dropHeadForUnstash()
            unstashed(ctx, next)
            next
          }
        }.take(math.min(numberOfMessages, size))
        interpretUnstashedMessages(behavior, ctx, iter, wrap)
      } finally {
        if (!unstashAlreadyInProgress)
          currentBehaviorWhenUnstashInProgress = OptionVal.None
      }
    }
  }

  private def interpretUnstashedMessages(
      behavior: Behavior[T],
      ctx: TypedActorContext[T],
      messages: Iterator[Node[T]],
      wrap: T => T): Behavior[T] = {
    @tailrec def interpretOne(b: Behavior[T]): Behavior[T] = {
      val b2 = Behavior.start(b, ctx)
      currentBehaviorWhenUnstashInProgress = OptionVal.Some(b2)
      if (!Behavior.isAlive(b2) || !messages.hasNext) b2
      else {
        val node = messages.next()
        val message = wrap(node.message)
        val interpretResult = try {
          message match {
            case sig: Signal => Behavior.interpretSignal(b2, ctx, sig)
            case msg         => interpretUnstashedMessage(b2, ctx, msg, node)
          }
        } catch {
          case NonFatal(e) => throw UnstashException(e, b2)
        }

        val actualNext =
          if (interpretResult == BehaviorImpl.same) b2
          else if (Behavior.isUnhandled(interpretResult)) {
            ctx.asScala.onUnhandled(message)
            b2
          } else {
            interpretResult
          }

        if (Behavior.isAlive(actualNext))
          interpretOne(Behavior.canonicalize(actualNext, b2, ctx)) // recursive
        else {
          unstashRestToDeadLetters(ctx, messages, wrap)
          actualNext
        }
      }
    }

    val started = Behavior.start(behavior, ctx)
    val actualInitialBehavior =
      if (Behavior.isUnhandled(started))
        throw new IllegalArgumentException("Cannot unstash with unhandled as starting behavior")
      else if (started == BehaviorImpl.same) {
        currentBehaviorWhenUnstashInProgress match {
          case OptionVal.Some(c) => c
          case _                 => ctx.asScala.currentBehavior
        }
      } else started

    if (Behavior.isAlive(actualInitialBehavior)) {
      interpretOne(actualInitialBehavior)
    } else {
      unstashRestToDeadLetters(ctx, messages, wrap)
      started
    }
  }

  private def unstashRestToDeadLetters(ctx: TypedActorContext[T], messages: Iterator[Node[T]], wrap: T => T): Unit = {
    val scalaCtx = ctx.asScala
    import akka.actor.typed.scaladsl.adapter._
    val classicDeadLetters = scalaCtx.system.deadLetters.toClassic
    messages.foreach(node =>
      scalaCtx.system.deadLetters[Any] ! DeadLetter(wrap(node.message), classicDeadLetters, ctx.asScala.self.toClassic))
  }

  override def unstash(behavior: Behavior[T], numberOfMessages: Int, wrap: JFunction[T, T]): Behavior[T] =
    unstash(behavior, numberOfMessages, x => wrap.apply(x))

  override def toString: String =
    s"StashBuffer($size/$capacity)"

  @InternalStableApi
  private[akka] def unstashed(ctx: ActorContext[T], node: Node[T]): Unit = ()

  @InternalStableApi
  private def stashCleared(ctx: ActorContext[T]): Unit = ()

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object UnstashException {
  def unwrap(t: Throwable): Throwable = t match {
    case UnstashException(e, _) => e
    case _                      => t
  }

}

/**
 * INTERNAL API:
 *
 * When unstashing, the exception is wrapped in UnstashException because supervisor strategy
 * and ActorAdapter need the behavior that threw. It will use the behavior in the `UnstashException`
 * to emit the PreRestart and PostStop to the right behavior and install the latest behavior for resume strategy.
 */
@InternalApi private[akka] final case class UnstashException[T](cause: Throwable, behavior: Behavior[T])
    extends RuntimeException(s"[$cause] when unstashing in [$behavior]", cause)
