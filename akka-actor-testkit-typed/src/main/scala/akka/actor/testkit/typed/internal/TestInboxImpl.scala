/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import java.util.concurrent.ConcurrentLinkedQueue

import scala.annotation.tailrec
import scala.collection.immutable

import akka.actor.{ ActorPath, Address, RootActorPath }
import akka.actor.typed.ActorRef
import akka.annotation.InternalApi
import akka.pattern.StatusReply
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class TestInboxImpl[T](path: ActorPath)
    extends akka.actor.testkit.typed.javadsl.TestInbox[T]
    with akka.actor.testkit.typed.scaladsl.TestInbox[T] {

  private val q = new ConcurrentLinkedQueue[T]

  override val ref: ActorRef[T] = new FunctionRef[T](path, (message, _) => q.add(message))
  override def getRef() = ref

  override def receiveMessage(): T = q.poll() match {
    case null => throw new NoSuchElementException(s"polling on an empty inbox: $path")
    case x    => x
  }

  override def expectMessage(expectedMessage: T): TestInboxImpl[T] = {
    q.poll() match {
      case null    => assert(assertion = false, s"expected message: $expectedMessage but no messages were received")
      case message => assert(message == expectedMessage, s"expected: $expectedMessage but received $message")
    }
    this
  }

  override protected def internalReceiveAll(): immutable.Seq[T] = {
    @tailrec def rec(acc: List[T]): List[T] = q.poll() match {
      case null => acc.reverse
      case x    => rec(x :: acc)
    }

    rec(Nil)
  }

  def hasMessages: Boolean = q.peek() != null

  @InternalApi private[akka] def as[U]: TestInboxImpl[U] = this.asInstanceOf[TestInboxImpl[U]]

}

/**
 * INTERNAL API
 */
@InternalApi
object TestInboxImpl {
  def apply[T](name: String): TestInboxImpl[T] = {
    new TestInboxImpl(address / name)
  }

  private[akka] val address = RootActorPath(Address("akka.actor.typed.inbox", "anonymous"))
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ReplyInboxImpl[T](private var underlying: OptionVal[TestInboxImpl[T]])
    extends akka.actor.testkit.typed.javadsl.ReplyInbox[T]
    with akka.actor.testkit.typed.scaladsl.ReplyInbox[T] {

  def receiveReply(): T =
    underlying match {
      case OptionVal.Some(testInbox) =>
        underlying = OptionVal.None
        testInbox.receiveMessage()

      case _ => throw new AssertionError("Reply was already received")
    }

  def expectReply(expectedReply: T): Unit =
    receiveReply() match {
      case matches if (matches == expectedReply) => ()
      case doesntMatch =>
        throw new AssertionError(s"Expected $expectedReply but received $doesntMatch")
    }

  def expectNoReply(): ReplyInboxImpl[T] =
    underlying match {
      case OptionVal.Some(testInbox) if (testInbox.hasMessages) =>
        throw new AssertionError(s"Expected no reply, but ${receiveReply()} was received")

      case OptionVal.Some(_) => this

      case _ =>
        // already received the reply, so this expectation shouldn't even be made
        throw new AssertionError("Improper expectation of no reply: reply was already received")
    }

  def hasReply: Boolean =
    underlying match {
      case OptionVal.Some(testInbox) => testInbox.hasMessages
      case _                         => false
    }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class StatusReplyInboxImpl[T](private var underlying: OptionVal[TestInboxImpl[StatusReply[T]]])
    extends akka.actor.testkit.typed.javadsl.StatusReplyInbox[T]
    with akka.actor.testkit.typed.scaladsl.StatusReplyInbox[T] {

  def receiveStatusReply(): StatusReply[T] =
    underlying match {
      case OptionVal.Some(testInbox) =>
        underlying = OptionVal.None
        testInbox.receiveMessage()

      case _ => throw new AssertionError("Reply was already received")
    }

  def receiveValue(): T =
    receiveStatusReply() match {
      case StatusReply.Success(v) => v.asInstanceOf[T]
      case err                    => throw new AssertionError(s"Expected a successful reply but received $err")
    }

  def receiveError(): Throwable =
    receiveStatusReply() match {
      case StatusReply.Error(t) => t
      case success              => throw new AssertionError(s"Expected an error reply but received $success")
    }

  def expectValue(expectedValue: T): Unit =
    receiveValue() match {
      case matches if (matches == expectedValue) => ()
      case doesntMatch =>
        throw new AssertionError(s"Expected $expectedValue but received $doesntMatch")
    }

  def expectErrorMessage(errorMessage: String): Unit =
    receiveError() match {
      case matches if (matches.getMessage == errorMessage) => ()
      case doesntMatch =>
        throw new AssertionError(s"Expected a throwable with message $errorMessage, but got ${doesntMatch.getMessage}")
    }

  def expectNoReply(): StatusReplyInboxImpl[T] =
    underlying match {
      case OptionVal.Some(testInbox) if (testInbox.hasMessages) =>
        throw new AssertionError(s"Expected no reply, but ${receiveStatusReply()} was received")

      case OptionVal.Some(_) => this

      case _ =>
        // already received the reply, so this expectation shouldn't even be made
        throw new AssertionError("Improper expectation of no reply: reply was already received")
    }

  def hasReply: Boolean =
    underlying match {
      case OptionVal.Some(testInbox) => testInbox.hasMessages
      case _                         => false
    }
}
