/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import java.util.concurrent.ConcurrentLinkedQueue

import scala.annotation.tailrec

import akka.{ actor => classic }
import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.internal.{ ActorRefImpl, SystemMessage }
import akka.actor.typed.internal.InternalRecipientRef
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class DebugRef[T](override val path: classic.ActorPath, override val isLocal: Boolean)
    extends ActorRef[T]
    with ActorRefImpl[T]
    with InternalRecipientRef[T] {

  private val q = new ConcurrentLinkedQueue[Either[SystemMessage, T]]

  override def tell(message: T): Unit = q.add(Right(message))
  override def sendSystem(signal: SystemMessage): Unit = q.add(Left(signal))

  def hasMessage: Boolean = q.peek match {
    case null     => false
    case Left(_)  => false
    case Right(_) => true
  }

  def hasSignal: Boolean = q.peek match {
    case null     => false
    case Left(_)  => true
    case Right(_) => false
  }

  def hasSomething: Boolean = q.peek != null

  def receiveMessage(): T = q.poll match {
    case null           => throw new NoSuchElementException("empty DebugRef")
    case Left(signal)   => throw new IllegalStateException(s"expected message but found signal $signal")
    case Right(message) => message
  }

  def receiveSignal(): SystemMessage = q.poll match {
    case null           => throw new NoSuchElementException("empty DebugRef")
    case Left(signal)   => signal
    case Right(message) => throw new IllegalStateException(s"expected signal but found message $message")
  }

  def receiveAll(): List[Either[SystemMessage, T]] = {
    @tailrec def rec(acc: List[Either[SystemMessage, T]]): List[Either[SystemMessage, T]] =
      q.poll match {
        case null  => acc.reverse
        case other => rec(other :: acc)
      }
    rec(Nil)
  }

  // impl InternalRecipientRef, ask not supported
  override def provider: ActorRefProvider = throw new UnsupportedOperationException("no provider")
  // impl InternalRecipientRef
  def isTerminated: Boolean = false
}
