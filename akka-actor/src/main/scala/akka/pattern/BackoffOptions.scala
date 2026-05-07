/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.jdk.DurationConverters._

import akka.actor.{ ActorRef, OneForOneStrategy, Props, SupervisorStrategy }
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.pattern.internal.{ BackoffOnRestartSupervisor, BackoffOnStopSupervisor }

/**
 * Backoff options allow to specify a number of properties for backoff supervisors.
 */
object BackoffOpts {

  /**
   * Back-off options for creating a back-off supervisor actor that expects a child actor to restart on failure.
   *
   * This explicit supervisor behaves similarly to the normal implicit supervision where
   * if an actor throws an exception, the decider on the supervisor will decide when to
   * `Stop`, `Restart`, `Escalate`, `Resume` the child actor.
   *
   * When the `Restart` directive is specified, the supervisor will delay the restart
   * using an exponential back off strategy (bounded by minBackoff and maxBackoff).
   *
   * This supervisor is intended to be transparent to both the child actor and external actors.
   * Where external actors can send messages to the supervisor as if it was the child and the
   * messages will be forwarded. And when the child is `Terminated`, the supervisor is also
   * `Terminated`.
   * Transparent to the child means that the child does not have to be aware that it is being
   * supervised specifically by this actor. Just like it does
   * not need to know when it is being supervised by the usual implicit supervisors.
   * The only caveat is that the `ActorRef` of the child is not stable, so any user storing the
   * `sender()` `ActorRef` from the child response may eventually not be able to communicate with
   * the stored `ActorRef`. In general all messages to the child should be directed through this actor.
   *
   * An example of where this supervisor might be used is when you may have an actor that is
   * responsible for continuously polling on a server for some resource that sometimes may be down.
   * Instead of hammering the server continuously when the resource is unavailable, the actor will
   * be restarted with an exponentially increasing back off until the resource is available again.
   *
   * '''***
   * This supervisor should not be used with `Akka Persistence` child actors.
   * `Akka Persistence` actors shutdown unconditionally on `persistFailure()`s rather
   * than throw an exception on a failure like normal actors.
   * [[#onStop]] should be used instead for cases where the child actor
   * terminates itself as a failure signal instead of the normal behavior of throwing an exception.
   * ***'''
   * You can define another
   * supervision strategy by using `akka.pattern.BackoffOptions.withSupervisorStrategy` on [[akka.pattern.BackoffOnFailureOptions]].
   *
   * @param childProps   the [[akka.actor.Props]] of the child actor that
   *                     will be started and supervised
   * @param childName    name of the child actor
   * @param minBackoff   minimum (initial) duration until the child actor will
   *                     started again, if it is terminated
   * @param maxBackoff   the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *                     random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *                     In order to skip this additional delay pass in `0`.
   */
  def onFailure(
      childProps: Props,
      childName: String,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): BackoffOnFailureOptions =
    BackoffOnFailureOptionsImpl(childProps, childName, minBackoff, maxBackoff, randomFactor)

  /**
   * Java API: Back-off options for creating a back-off supervisor actor that expects a child actor to restart on failure.
   *
   * This explicit supervisor behaves similarly to the normal implicit supervision where
   * if an actor throws an exception, the decider on the supervisor will decide when to
   * `Stop`, `Restart`, `Escalate`, `Resume` the child actor.
   *
   * When the `Restart` directive is specified, the supervisor will delay the restart
   * using an exponential back off strategy (bounded by minBackoff and maxBackoff).
   *
   * This supervisor is intended to be transparent to both the child actor and external actors.
   * Where external actors can send messages to the supervisor as if it was the child and the
   * messages will be forwarded. And when the child is `Terminated`, the supervisor is also
   * `Terminated`.
   * Transparent to the child means that the child does not have to be aware that it is being
   * supervised specifically by this actor. Just like it does
   * not need to know when it is being supervised by the usual implicit supervisors.
   * The only caveat is that the `ActorRef` of the child is not stable, so any user storing the
   * `sender()` `ActorRef` from the child response may eventually not be able to communicate with
   * the stored `ActorRef`. In general all messages to the child should be directed through this actor.
   *
   * An example of where this supervisor might be used is when you may have an actor that is
   * responsible for continuously polling on a server for some resource that sometimes may be down.
   * Instead of hammering the server continuously when the resource is unavailable, the actor will
   * be restarted with an exponentially increasing back off until the resource is available again.
   *
   * '''***
   * This supervisor should not be used with `Akka Persistence` child actors.
   * `Akka Persistence` actors shutdown unconditionally on `persistFailure()`s rather
   * than throw an exception on a failure like normal actors.
   * [[#onStop]] should be used instead for cases where the child actor
   * terminates itself as a failure signal instead of the normal behavior of throwing an exception.
   * ***'''
   * You can define another
   * supervision strategy by using `akka.pattern.BackoffOptions.withSupervisorStrategy` on [[akka.pattern.BackoffOnFailureOptions]].
   *
   * @param childProps     the [[akka.actor.Props]] of the child actor that
   *                       will be started and supervised
   * @param childName      name of the child actor
   * @param minBackoff     minimum (initial) duration until the child actor will
   *                       started again, if it is terminated
   * @param maxBackoff     the exponential back-off is capped to this duration
   * @param randomFactor   after calculation of the exponential back-off an additional
   *                       random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *                       In order to skip this additional delay pass in `0`.
   */
  def onFailure(
      childProps: Props,
      childName: String,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): BackoffOnFailureOptions =
    onFailure(childProps, childName, minBackoff.toScala, maxBackoff.toScala, randomFactor)

  /**
   * Back-off options for creating a back-off supervisor actor that expects a child actor to stop on failure.
   *
   * This actor can be used to supervise a child actor and start it again
   * after a back-off duration if the child actor is stopped.
   *
   * This is useful in situations where the re-start of the child actor should be
   * delayed e.g. in order to give an external resource time to recover before the
   * child actor tries contacting it again (after being restarted).
   *
   * Specifically this pattern is useful for persistent actors,
   * which are stopped in case of persistence failures.
   * Just restarting them immediately would probably fail again (since the data
   * store is probably unavailable). It is better to try again after a delay.
   *
   * It supports exponential back-off between the given `minBackoff` and
   * `maxBackoff` durations. For example, if `minBackoff` is 3 seconds and
   * `maxBackoff` 30 seconds the start attempts will be delayed with
   * 3, 6, 12, 24, 30, 30 seconds. The exponential back-off counter is reset
   * if the actor is not terminated within the `minBackoff` duration.
   *
   * In addition to the calculated exponential back-off an additional
   * random delay based the given `randomFactor` is added, e.g. 0.2 adds up to 20%
   * delay. The reason for adding a random delay is to avoid that all failing
   * actors hit the backend resource at the same time.
   *
   * You can retrieve the current child `ActorRef` by sending `BackoffSupervisor.GetCurrentChild`
   * message to this actor and it will reply with [[akka.pattern.BackoffSupervisor.CurrentChild]]
   * containing the `ActorRef` of the current child, if any.
   *
   * The `BackoffSupervisor`delegates all messages from the child to the parent of the
   * `BackoffSupervisor`, with the supervisor as sender.
   *
   * The `BackoffSupervisor` forwards all other messages to the child, if it is currently running.
   *
   * The child can stop itself and send a [[akka.actor.PoisonPill]] to the parent supervisor
   * if it wants to do an intentional stop.
   *
   * Exceptions in the child are handled with the default supervisionStrategy, which can be changed by using
   * [[BackoffOnStopOptions#withSupervisorStrategy]] or [[BackoffOnStopOptions#withDefaultStoppingStrategy]]. A
   * `Restart` will perform a normal immediate restart of the child. A `Stop` will
   * stop the child, but it will be started again after the back-off duration.
   *
   * @param childProps   the [[akka.actor.Props]] of the child actor that
   *                     will be started and supervised
   * @param childName    name of the child actor
   * @param minBackoff   minimum (initial) duration until the child actor will
   *                     started again, if it is terminated
   * @param maxBackoff   the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *                     random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *                     In order to skip this additional delay pass in `0`.
   */
  def onStop(
      childProps: Props,
      childName: String,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): BackoffOnStopOptions =
    BackoffOnStopOptionsImpl(childProps, childName, minBackoff, maxBackoff, randomFactor)

  /**
   * Java API: Back-off options for creating a back-off supervisor actor that expects a child actor to stop on failure.
   *
   * This actor can be used to supervise a child actor and start it again
   * after a back-off duration if the child actor is stopped.
   *
   * This is useful in situations where the re-start of the child actor should be
   * delayed e.g. in order to give an external resource time to recover before the
   * child actor tries contacting it again (after being restarted).
   *
   * Specifically this pattern is useful for persistent actors,
   * which are stopped in case of persistence failures.
   * Just restarting them immediately would probably fail again (since the data
   * store is probably unavailable). It is better to try again after a delay.
   *
   * It supports exponential back-off between the given `minBackoff` and
   * `maxBackoff` durations. For example, if `minBackoff` is 3 seconds and
   * `maxBackoff` 30 seconds the start attempts will be delayed with
   * 3, 6, 12, 24, 30, 30 seconds. The exponential back-off counter is reset
   * if the actor is not terminated within the `minBackoff` duration.
   *
   * In addition to the calculated exponential back-off an additional
   * random delay based the given `randomFactor` is added, e.g. 0.2 adds up to 20%
   * delay. The reason for adding a random delay is to avoid that all failing
   * actors hit the backend resource at the same time.
   *
   * You can retrieve the current child `ActorRef` by sending `BackoffSupervisor.GetCurrentChild`
   * message to this actor and it will reply with [[akka.pattern.BackoffSupervisor.CurrentChild]]
   * containing the `ActorRef` of the current child, if any.
   *
   * The `BackoffSupervisor`delegates all messages from the child to the parent of the
   * `BackoffSupervisor`, with the supervisor as sender.
   *
   * The `BackoffSupervisor` forwards all other messages to the child, if it is currently running.
   *
   * The child can stop itself and send a [[akka.actor.PoisonPill]] to the parent supervisor
   * if it wants to do an intentional stop.
   *
   * Exceptions in the child are handled with the default supervisionStrategy, which can be changed by using
   * [[BackoffOnStopOptions#withSupervisorStrategy]] or [[BackoffOnStopOptions#withDefaultStoppingStrategy]]. A
   * `Restart` will perform a normal immediate restart of the child. A `Stop` will
   * stop the child, but it will be started again after the back-off duration.
   *
   * @param childProps     the [[akka.actor.Props]] of the child actor that
   *                       will be started and supervised
   * @param childName      name of the child actor
   * @param minBackoff     minimum (initial) duration until the child actor will
   *                       started again, if it is terminated
   * @param maxBackoff     the exponential back-off is capped to this duration
   * @param randomFactor   after calculation of the exponential back-off an additional
   *                       random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *                       In order to skip this additional delay pass in `0`.
   */
  def onStop(
      childProps: Props,
      childName: String,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): BackoffOnStopOptions =
    onStop(childProps, childName, minBackoff.toScala, maxBackoff.toScala, randomFactor)
}

/**
 * Not for user extension
 */
@DoNotInherit
private[akka] sealed trait ExtendedBackoffOptions[T <: ExtendedBackoffOptions[T]] {

  /**
   * Returns a new BackoffOptions with automatic back-off reset.
   * The back-off algorithm is reset if the child does not crash within the specified `resetBackoff`.
   *
   * @param resetBackoff The back-off is reset if the child does not crash within this duration.
   */
  def withAutoReset(resetBackoff: FiniteDuration): T

  /**
   * Returns a new BackoffOptions with manual back-off reset. The back-off is only reset
   * if the child sends a `BackoffSupervisor.Reset` to its parent (the backoff-supervisor actor).
   */
  def withManualReset: T

  /**
   * Returns a new BackoffOptions with the supervisorStrategy.
   *
   * @param supervisorStrategy the supervisorStrategy that the back-off supervisor will use.
   *                           The default supervisor strategy is used as fallback if the specified supervisorStrategy (its decider)
   *                           does not explicitly handle an exception. As the BackoffSupervisor creates a separate actor to handle the
   *                           backoff process, only a [[OneForOneStrategy]] makes sense here.
   *                           Note that changing the strategy will replace the previously defined maxNrOfRetries.
   */
  def withSupervisorStrategy(supervisorStrategy: OneForOneStrategy): T

  /**
   * Returns a new BackoffOptions with a maximum number of retries to restart the child actor.
   * By default, the supervisor will retry infinitely.
   * With this option, the supervisor will terminate itself after the maxNoOfRetries is reached.
   *
   * @param maxNrOfRetries the number of times a child actor is allowed to be restarted.
   *                       If negative, the value is unbounded, otherwise the provided
   *                       limit is used. If the limit is exceeded the child actor will be stopped.
   */
  def withMaxNrOfRetries(maxNrOfRetries: Int): T

  /**
   * Returns a new BackoffOptions with a constant reply to messages that the supervisor receives while its
   * child is stopped. By default, a message received while the child is stopped is forwarded to `deadLetters`.
   * With this option, the supervisor will reply to the sender instead.
   *
   * @param replyWhileStopped The message that the supervisor will send in response to all messages while
   *                          its child is stopped.
   */
  def withReplyWhileStopped(replyWhileStopped: Any): T

  /**
   * Returns a new BackoffOptions with a custom handler for messages that the supervisor receives while its child is stopped.
   * By default, a message received while the child is stopped is forwarded to `deadLetters`.
   * Essentially, this handler replaces `deadLetters` allowing to implement custom handling instead of a static reply.
   *
   * @param handler PartialFunction of the received message and sender
   */
  def withHandlerWhileStopped(handler: ActorRef): T

  /**
   * Returns the props to create the back-off supervisor.
   */
  private[akka] def props: Props
}

@DoNotInherit
sealed trait BackoffOnStopOptions extends ExtendedBackoffOptions[BackoffOnStopOptions] {

  /**
   * Returns a new BackoffOptions with a default `SupervisorStrategy.stoppingStrategy`.
   * The default supervisor strategy is used as fallback for throwables not handled by `SupervisorStrategy.stoppingStrategy`.
   */
  def withDefaultStoppingStrategy: BackoffOnStopOptions

  /**
   * Predicate evaluated for each message, if it returns true and the supervised actor is
   * stopped then the supervisor will stop its self. If it returns true while
   * the supervised actor is running then it will be forwarded to the supervised actor and
   * when the supervised actor stops its self the supervisor will stop its self.
   */
  def withFinalStopMessage(isFinalStopMessage: Any => Boolean): BackoffOnStopOptions
}

@DoNotInherit
sealed trait BackoffOnFailureOptions extends ExtendedBackoffOptions[BackoffOnFailureOptions]

private final case class BackoffOnStopOptionsImpl[T](
    childProps: Props,
    childName: String,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    reset: Option[BackoffReset] = None,
    supervisorStrategy: OneForOneStrategy = OneForOneStrategy()(SupervisorStrategy.defaultStrategy.decider),
    handlingWhileStopped: HandlingWhileStopped = ForwardDeathLetters,
    finalStopMessage: Option[Any => Boolean] = None)
    extends BackoffOnStopOptions {

  private val backoffReset = reset.getOrElse(AutoReset(minBackoff))

  // default
  def withAutoReset(resetBackoff: FiniteDuration) = copy(reset = Some(AutoReset(resetBackoff)))
  def withManualReset = copy(reset = Some(ManualReset))
  def withSupervisorStrategy(supervisorStrategy: OneForOneStrategy) = copy(supervisorStrategy = supervisorStrategy)
  def withReplyWhileStopped(replyWhileStopped: Any) = copy(handlingWhileStopped = ReplyWith(replyWhileStopped))
  def withHandlerWhileStopped(handlerWhileStopped: ActorRef) =
    copy(handlingWhileStopped = ForwardTo(handlerWhileStopped))
  def withMaxNrOfRetries(maxNrOfRetries: Int) =
    copy(supervisorStrategy = supervisorStrategy.withMaxNrOfRetries(maxNrOfRetries))

  // additional
  def withDefaultStoppingStrategy =
    copy(
      supervisorStrategy =
        OneForOneStrategy(supervisorStrategy.maxNrOfRetries)(SupervisorStrategy.stoppingStrategy.decider))
  def withFinalStopMessage(action: Any => Boolean) = copy(finalStopMessage = Some(action))

  def props: Props = {
    require(minBackoff > Duration.Zero, "minBackoff must be > 0")
    require(maxBackoff >= minBackoff, "maxBackoff must be >= minBackoff")
    require(0.0 <= randomFactor && randomFactor <= 1.0, "randomFactor must be between 0.0 and 1.0")
    backoffReset match {
      case AutoReset(resetBackoff) =>
        require(minBackoff <= resetBackoff && resetBackoff <= maxBackoff)
      case _ => // ignore
    }

    Props(
      new BackoffOnStopSupervisor(
        childProps,
        childName,
        minBackoff,
        maxBackoff,
        backoffReset,
        randomFactor,
        supervisorStrategy,
        handlingWhileStopped,
        finalStopMessage))
  }
}

private final case class BackoffOnFailureOptionsImpl[T](
    childProps: Props,
    childName: String,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    reset: Option[BackoffReset] = None,
    supervisorStrategy: OneForOneStrategy = OneForOneStrategy()(SupervisorStrategy.defaultStrategy.decider),
    handlingWhileStopped: HandlingWhileStopped = ForwardDeathLetters)
    extends BackoffOnFailureOptions {

  private val backoffReset = reset.getOrElse(AutoReset(minBackoff))

  // default
  def withAutoReset(resetBackoff: FiniteDuration) = copy(reset = Some(AutoReset(resetBackoff)))
  def withManualReset = copy(reset = Some(ManualReset))
  def withSupervisorStrategy(supervisorStrategy: OneForOneStrategy) = copy(supervisorStrategy = supervisorStrategy)
  def withReplyWhileStopped(replyWhileStopped: Any) = copy(handlingWhileStopped = ReplyWith(replyWhileStopped))
  def withHandlerWhileStopped(handlerWhileStopped: ActorRef) =
    copy(handlingWhileStopped = ForwardTo(handlerWhileStopped))
  def withMaxNrOfRetries(maxNrOfRetries: Int) =
    copy(supervisorStrategy = supervisorStrategy.withMaxNrOfRetries(maxNrOfRetries))

  def props: Props = {
    require(minBackoff > Duration.Zero, "minBackoff must be > 0")
    require(maxBackoff >= minBackoff, "maxBackoff must be >= minBackoff")
    require(0.0 <= randomFactor && randomFactor <= 1.0, "randomFactor must be between 0.0 and 1.0")
    backoffReset match {
      case AutoReset(resetBackoff) =>
        require(minBackoff <= resetBackoff && resetBackoff <= maxBackoff)
      case _ => // ignore
    }

    Props(
      new BackoffOnRestartSupervisor(
        childProps,
        childName,
        minBackoff,
        maxBackoff,
        backoffReset,
        randomFactor,
        supervisorStrategy,
        handlingWhileStopped))
  }
}

@InternalApi
private[akka] sealed trait BackoffReset
private[akka] case object ManualReset extends BackoffReset
private[akka] final case class AutoReset(resetBackoff: FiniteDuration) extends BackoffReset

@InternalApi
private[akka] sealed trait HandlingWhileStopped
private[akka] case object ForwardDeathLetters extends HandlingWhileStopped
private[akka] case class ForwardTo(handler: ActorRef) extends HandlingWhileStopped
private[akka] case class ReplyWith(msg: Any) extends HandlingWhileStopped
