/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.scaladsl

import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.reflect.ClassTag

import com.typesafe.config.Config

import akka.actor.testkit.typed.{ CapturedLogEvent, Effect }
import akka.actor.testkit.typed.internal.{ ActorSystemStub, BehaviorTestKitImpl }
import akka.actor.typed.{ ActorRef, Behavior, Signal, TypedActorContext }
import akka.actor.typed.receptionist.Receptionist
import akka.annotation.{ ApiMayChange, DoNotInherit }
import akka.pattern.StatusReply

@ApiMayChange
object BehaviorTestKit {

  val ApplicationTestConfig: Config = ActorTestKit.ApplicationTestConfig

  def apply[T](initialBehavior: Behavior[T], name: String, config: Config): BehaviorTestKit[T] = {
    val system = new ActorSystemStub("StubbedActorContext", config)
    val uid = ThreadLocalRandom.current().nextInt()
    new BehaviorTestKitImpl(system, (system.path / name).withUid(uid), initialBehavior)
  }
  def apply[T](initialBehavior: Behavior[T], name: String): BehaviorTestKit[T] = {
    apply(initialBehavior, name, ActorSystemStub.config.defaultReference)
  }
  def apply[T](initialBehavior: Behavior[T]): BehaviorTestKit[T] =
    apply(initialBehavior, "testkit")

}

/**
 * Used for synchronous testing [[akka.actor.typed.Behavior]]s. Stores all effects e.g. Spawning of children,
 * watching and offers access to what effects have taken place.
 *
 * For asynchronous testing of `Behavior`s running see [[ActorTestKit]]
 *
 * Not for user extension. See `BehaviorTestKit.apply` factory methods
 */
@DoNotInherit
@ApiMayChange
trait BehaviorTestKit[T] {

  /**
   * Constructs a message using the provided function to inject a single-use "reply to" [[akka.actor.typed.ActorRef]],
   * and sends the constructed message to the behavior, recording any [[Effect]]s.
   *
   * The returned [[ReplyInbox]] allows the message sent to the "reply to" `ActorRef` to be asserted on.
   */
  def runAsk[Res](f: ActorRef[Res] => T): ReplyInbox[Res]

  /**
   * The same as [[runAsk]] but only for requests that result in a response of type [[akka.pattern.StatusReply]].
   */
  def runAskWithStatus[Res](f: ActorRef[StatusReply[Res]] => T): StatusReplyInbox[Res]

  // FIXME it is weird that this is public but it is used in BehaviorSpec, could we avoid that?
  private[akka] def context: TypedActorContext[T]

  /**
   * Requests the oldest [[Effect]] or [[akka.actor.testkit.typed.Effect.NoEffects]] if no effects
   * have taken place. The effect is consumed, subsequent calls won't
   * will not include this effect.
   */
  def retrieveEffect(): Effect

  /**
   * Get the child inbox for the child with the given name, or fail if there is no child with the given name
   * spawned
   */
  def childInbox[U](name: String): TestInbox[U]

  /**
   * Get the child inbox for the child ActorRef, or fail if there is no such child.
   */
  def childInbox[U](child: ActorRef[U]): TestInbox[U]

  /**
   * Get the [[akka.actor.typed.Behavior]] testkit for the given child [[akka.actor.typed.ActorRef]].
   */
  def childTestKit[U](child: ActorRef[U]): BehaviorTestKit[U]

  /**
   * The self inbox contains messages the behavior sent to `context.self`
   */
  def selfInbox(): TestInbox[T]

  /**
   * The self reference of the actor living inside this testkit.
   */
  def ref: ActorRef[T] = selfInbox().ref

  /**
   * Requests all the effects. The effects are consumed, subsequent calls will only
   * see new effects.
   */
  def retrieveAllEffects(): immutable.Seq[Effect]

  /**
   * Returns if there have been any effects.
   */
  def hasEffects(): Boolean

  /**
   * Asserts that the oldest effect is the expectedEffect. Removing it from
   * further assertions.
   */
  def expectEffect(expectedEffect: Effect): Unit

  /**
   * Asserts that the oldest effect is of type T. Consumes and returns the concrete effect for
   * further direct assertions.
   */
  def expectEffectType[E <: Effect](implicit classTag: ClassTag[E]): E

  /**
   * Asserts that the oldest effect matches the given partial function.
   */
  def expectEffectPF[R](f: PartialFunction[Effect, R]): R

  /**
   * The current behavior, can change any time `run` is called
   */
  def currentBehavior: Behavior[T]

  /**
   * Returns the current behavior as it was returned from processing the previous message.
   * For example if [[Behaviors.unhandled]] is returned it will be kept here, but not in
   * [[currentBehavior]].
   */
  def returnedBehavior: Behavior[T]

  /**
   * Is the current behavior alive or stopped
   */
  def isAlive: Boolean

  /**
   * Send the message to the behavior and record any [[Effect]]s
   */
  def run(message: T): Unit

  /**
   * Send the first message in the selfInbox to the behavior and run it, recording [[Effect]]s.
   */
  def runOne(): Unit

  /**
   * Send the signal to the behavior and record any [[Effect]]s
   */
  def signal(signal: Signal): Unit

  /**
   * Returns all the [[CapturedLogEvent]] issued by this behavior(s)
   */
  def logEntries(): immutable.Seq[CapturedLogEvent]

  /**
   * Clear the log entries
   */
  def clearLog(): Unit

  /**
   * The receptionist inbox contains messages sent to `system.receptionist`
   */
  def receptionistInbox(): TestInbox[Receptionist.Command]
}
