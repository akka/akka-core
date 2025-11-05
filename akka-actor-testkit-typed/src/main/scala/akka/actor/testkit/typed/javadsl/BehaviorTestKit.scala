/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.nowarn

import com.typesafe.config.Config

import akka.actor.testkit.typed.{ CapturedLogEvent, Effect }
import akka.actor.testkit.typed.internal.{ ActorSystemStub, BehaviorTestKitImpl }
import akka.actor.typed.{ ActorRef, Behavior, Signal }
import akka.actor.typed.receptionist.Receptionist
import akka.annotation.{ ApiMayChange, DoNotInherit }
import akka.japi.function.{ Function => JFunction }
import akka.pattern.StatusReply

object BehaviorTestKit {

  /**
   * JAVA API
   */
  @ApiMayChange
  def applicationTestConfig: Config = akka.actor.testkit.typed.scaladsl.BehaviorTestKit.ApplicationTestConfig

  /**
   * JAVA API
   */
  @ApiMayChange
  def create[T](initialBehavior: Behavior[T], name: String, config: Config): BehaviorTestKit[T] = {
    val system = new ActorSystemStub("StubbedActorContext", config)
    val uid = ThreadLocalRandom.current().nextInt()
    new BehaviorTestKitImpl(system, (system.path / name).withUid(uid), initialBehavior)
  }

  /**
   * JAVA API
   */
  @ApiMayChange
  def create[T](initialBehavior: Behavior[T], name: String): BehaviorTestKit[T] = {
    create(initialBehavior, name, ActorSystemStub.config.defaultReference)
  }

  /**
   * JAVA API
   */
  @ApiMayChange
  def create[T](initialBehavior: Behavior[T]): BehaviorTestKit[T] =
    create(initialBehavior, "testkit")

}

/**
 * Used for synchronous testing [[akka.actor.typed.Behavior]]s. Stores all effects e.g. Spawning of children,
 * watching and offers access to what effects have taken place.
 *
 * Not for user extension or instantiation. See `BehaviorTestKit.create` factory methods
 *
 * For asynchronous testing of `Behavior`s running see [[ActorTestKit]]
 */
@DoNotInherit
@ApiMayChange
abstract class BehaviorTestKit[T] {

  /**
   * Constructs a message using the provided 'messageFactory' to inject a single-use "reply to"
   * [[akka.actor.typed.ActorRef]], and sends the constructed message to the behavior, recording any [[Effect]]s.
   *
   * The returned [[ReplyInbox]] allows the message sent to the "reply to" `ActorRef` to be asserted on.
   */
  def runAsk[Res](messageFactory: JFunction[ActorRef[Res], T]): ReplyInbox[Res]

  /**
   * The same as [[runAsk]], but with the response class specified.  This improves type inference in Java
   * when asserting on the reply in the same statement as the `runAsk` as in:
   *
   * ```
   * testkit.runAsk(Done.class, DoSomethingCommand::new).expectReply(Done.getInstance());
   * ```
   *
   * If explicitly saving the [[ReplyInbox]] in a variable, the version without the class may be preferred.
   */
  @nowarn("msg=never used") // responseClass is a pretend param to guide inference
  def runAsk[Res](responseClass: Class[Res], messageFactory: JFunction[ActorRef[Res], T]): ReplyInbox[Res] =
    runAsk(messageFactory)

  /**
   * The same as [[runAsk]] but only for requests that result in a response of type [[akka.pattern.StatusReply]].
   */
  def runAskWithStatus[Res](messageFactory: JFunction[ActorRef[StatusReply[Res]], T]): StatusReplyInbox[Res]

  /**
   * The same as [[runAskWithStatus]], but with the response class specified.  This improves type inference in
   * Java when asserting on the reply in the same statement as the `runAskWithStatus` as in:
   *
   * ```
   * testkit.runAskWithStatus(Done.class, DoSomethingWithStatusCommand::new).expectValue(Done.getInstance());
   * ```
   *
   * If explicitly saving the [[StatusReplyInbox]] in a variable, the version without the class may be preferred.
   */
  @nowarn("msg=never used") // responseClass is a pretend param to guide inference
  def runAskWithStatus[Res](
      responseClass: Class[Res],
      messageFactory: JFunction[ActorRef[StatusReply[Res]], T]): StatusReplyInbox[Res] =
    runAskWithStatus(messageFactory)

  /**
   * Requests the oldest [[Effect]] or [[akka.actor.testkit.typed.javadsl.Effects.noEffects]] if no effects
   * have taken place. The effect is consumed, subsequent calls won't
   * will not include this effect.
   */
  def getEffect(): Effect

  /**
   * Get the child inbox for the child with the given name, or fail if there is no child with the given name
   * spawned
   */
  def childInbox[U](name: String): TestInbox[U]

  /**
   * Get the child inbox for the child with the given name, or fail if there is no child with the given name
   * spawned
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
  def getRef(): ActorRef[T] = selfInbox().getRef()

  /**
   * Requests all the effects. The effects are consumed, subsequent calls will only
   * see new effects.
   */
  def getAllEffects(): java.util.List[Effect]

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
   * Asserts that the oldest effect is an instance of of class T. Consumes and returns the concrete effect for
   * further direct assertions.
   */
  def expectEffectClass[U <: Effect](effectClass: Class[U]): U

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
   * Send the signal to the beheavior and record any [[Effect]]s
   */
  def signal(signal: Signal): Unit

  /**
   * Returns all the [[CapturedLogEvent]] issued by this behavior(s)
   */
  def getAllLogEntries(): java.util.List[CapturedLogEvent]

  /**
   * Clear the log entries
   */
  def clearLog(): Unit

  /**
   * The receptionist inbox contains messages sent to `system.receptionist`
   */
  def receptionistInbox(): TestInbox[Receptionist.Command]
}
