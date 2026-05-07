/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import scala.collection.immutable

import akka.actor.testkit.typed.internal.TestInboxImpl
import akka.actor.typed.ActorRef
import akka.annotation.{ ApiMayChange, DoNotInherit }
import akka.pattern.StatusReply
import scala.jdk.CollectionConverters._

object TestInbox {
  def create[T](name: String): TestInbox[T] = TestInboxImpl(name)
  def create[T](): TestInbox[T] = TestInboxImpl("inbox")
}

/**
 * Utility for use as an [[ActorRef]] when *synchronously* testing [[akka.actor.typed.Behavior]]
 * with [[akka.actor.testkit.typed.javadsl.BehaviorTestKit]].
 *
 * If you plan to use a real [[akka.actor.typed.ActorSystem]] then use [[akka.actor.testkit.typed.javadsl.TestProbe]]
 * for asynchronous testing.
 *
 * Use `TestInbox.create` factory methods to create instances
 *
 * Not for user extension
 */
@DoNotInherit
abstract class TestInbox[T] {

  /**
   * The actor ref of the inbox
   */
  def getRef(): ActorRef[T]

  /**
   * Get and remove the oldest message
   */
  def receiveMessage(): T

  /**
   * Assert and remove the the oldest message.
   */
  def expectMessage(expectedMessage: T): TestInbox[T]

  /**
   * Collect all messages in the inbox and clear it out
   */
  def getAllReceived(): java.util.List[T] = internalReceiveAll().asJava

  protected def internalReceiveAll(): immutable.Seq[T]

  def hasMessages: Boolean

  // TODO expectNoMsg etc
}

/**
 * Similar to an [[akka.actor.testkit.typed.javadsl.TestInbox]], but can only ever give access to a single message (a reply).
 *
 * Not intended for user creation: the [[akka.actor.testkit.typed.javadsl.BehaviorTestKit]] will provide these to
 * denote that at most a single reply is expected.
 */
@DoNotInherit
@ApiMayChange
trait ReplyInbox[T] {

  /**
   * Get and remove the reply.  Subsequent calls to `receiveReply`, `expectReply`, and `expectNoReply` will fail and `hasReplies`
   * will be false after calling this method
   */
  def receiveReply(): T

  /**
   * Assert and remove the message.  Subsequent calls to `receiveReply`, `expectReply`, and `expectNoReply` will fail and `hasReplies`
   * will be false after calling this method
   */
  def expectReply(expectedReply: T): Unit

  def expectNoReply(): ReplyInbox[T]
  def hasReply: Boolean
}

/**
 * A [[akka.actor.testkit.typed.javadsl.ReplyInbox]] which specially handles [[akka.pattern.StatusReply]].
 *
 * Note that there is no provided ability to expect a specific `Throwable`, as it's recommended to prefer
 * a string error message or to enumerate failures with specific types.
 *
 * Not intended for user creation: the [[akka.actor.testkit.typed.javadsl.BehaviorTestKit]] will provide these to
 * denote that at most a single reply is expected.
 */
@DoNotInherit
@ApiMayChange
trait StatusReplyInbox[T] {

  /**
   * Get and remove the status reply.  Subsequent calls to any `receive` or `expect` method will fail and `hasReply`
   * will be false after calling this method.
   */
  def receiveStatusReply(): StatusReply[T]

  /**
   * Get and remove the successful value of the status reply.  This will fail if the status reply is an error.
   * Subsequent calls to any `receive` or `expect` method will fail and `hasReply` will be false after calling this
   * method.
   */
  def receiveValue(): T

  /**
   * Get and remove the error value of the status reply.  This will fail if the status reply is a success.
   * Subsequent calls to any `receive` or `expect` method will fail and `hasReply` will be false after calling this
   * method.
   */
  def receiveError(): Throwable

  /**
   * Assert that the status reply is a success with this value and remove the status reply.  Subsequent calls to any
   * `receive` or `expect` method will fail and `hasReply` will be false after calling this method.
   */
  def expectValue(expectedValue: T): Unit

  /**
   * Assert that the status reply is a failure with this error message and remove the status reply.  Subsequent
   * calls to any `receive` or `expect` method will fail and `hasReply` will be false after calling this method.
   */
  def expectErrorMessage(errorMessage: String): Unit

  /**
   * Assert that this inbox has *never* received a reply.
   */
  def expectNoReply(): StatusReplyInbox[T]

  def hasReply: Boolean
}
