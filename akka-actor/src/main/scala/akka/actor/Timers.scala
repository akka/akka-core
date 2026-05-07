/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.annotation.DoNotInherit
import akka.dispatch.Envelope
import akka.util.OptionVal

/**
 * Scala API: Mix in Timers into your Actor to get support for scheduled
 * `self` messages via [[TimerScheduler]].
 *
 * Timers are bound to the lifecycle of the actor that owns it,
 * and thus are cancelled automatically when it is restarted or stopped.
 */
trait Timers extends Actor {

  private def actorCell = context.asInstanceOf[ActorCell]
  private val _timers = new TimerSchedulerImpl(context)

  /**
   * Start and cancel timers via the enclosed `TimerScheduler`.
   */
  final def timers: TimerScheduler = _timers

  override protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    timers.cancelAll()
    super.aroundPreRestart(reason, message)
  }

  override protected[akka] def aroundPostStop(): Unit = {
    timers.cancelAll()
    super.aroundPostStop()
  }

  override protected[akka] def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    msg match {
      case timerMsg: TimerSchedulerImpl.TimerMsg =>
        _timers.interceptTimerMsg(timerMsg) match {
          case OptionVal.Some(m: AutoReceivedMessage) =>
            context.asInstanceOf[ActorCell].autoReceiveMessage(Envelope(m, self, context.system))
          case OptionVal.Some(m) =>
            if (this.isInstanceOf[Stash]) {
              // this is important for stash interaction, as stash will look directly at currentMessage #24557
              actorCell.currentMessage = actorCell.currentMessage.copy(message = m)
            }
            super.aroundReceive(receive, m)
          case _ => // discard
        }
      case _ =>
        super.aroundReceive(receive, msg)
    }
  }

}

/**
 * Java API: Support for scheduled `self` messages via [[TimerScheduler]].
 *
 * Timers are bound to the lifecycle of the actor that owns it,
 * and thus are cancelled automatically when it is restarted or stopped.
 */
abstract class AbstractActorWithTimers extends AbstractActor with Timers {

  /**
   * Start and cancel timers via the enclosed `TimerScheduler`.
   */
  final def getTimers: TimerScheduler = timers
}

/**
 * Support for scheduled `self` messages in an actor.
 * It is used by mixing in trait `Timers` in Scala or extending `AbstractActorWithTimers`
 * in Java.
 *
 * Timers are bound to the lifecycle of the actor that owns it,
 * and thus are cancelled automatically when it is restarted or stopped.
 *
 * `TimerScheduler` is not thread-safe, i.e. it must only be used within
 * the actor that owns it.
 */
@DoNotInherit abstract class TimerScheduler {

  /**
   * Scala API: Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startTimerWithFixedDelay(key: Any, msg: Any, delay: FiniteDuration): Unit

  /**
   * Scala API: Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages after the `initialDelay`.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startTimerWithFixedDelay(key: Any, msg: Any, initialDelay: FiniteDuration, delay: FiniteDuration): Unit

  /**
   * Java API: Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  final def startTimerWithFixedDelay(key: Any, msg: Any, delay: java.time.Duration): Unit =
    startTimerWithFixedDelay(key, msg, delay.toScala)

  /**
   * Java API: Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages after the `initialDelay`.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  final def startTimerWithFixedDelay(
      key: Any,
      msg: Any,
      initialDelay: java.time.Duration,
      delay: java.time.Duration): Unit =
    startTimerWithFixedDelay(key, msg, initialDelay.toScala, delay.toScala)

  /**
   * Scala API: Schedules a message to be sent repeatedly to the `self` actor with a
   * given frequency.
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `startTimerAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `startTimerWithFixedDelay` is often preferred.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startTimerAtFixedRate(key: Any, msg: Any, interval: FiniteDuration): Unit

  /**
   * Scala API: Schedules a message to be sent repeatedly to the `self` actor with a
   * given frequency.
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval` after `initialDelay`.
   *
   * Warning: `startTimerAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `startTimerWithFixedDelay` is often preferred.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startTimerAtFixedRate(key: Any, msg: Any, initialDelay: FiniteDuration, interval: FiniteDuration): Unit

  /**
   * Java API: Schedules a message to be sent repeatedly to the `self` actor with a
   * given frequency.
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `startTimerAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `startTimerWithFixedDelay` is often preferred.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  final def startTimerAtFixedRate(key: Any, msg: Any, interval: java.time.Duration): Unit =
    startTimerAtFixedRate(key, msg, interval.toScala)

  /**
   * Java API: Schedules a message to be sent repeatedly to the `self` actor with a
   * given frequency.
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `startTimerAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `startTimerWithFixedDelay` is often preferred.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  final def startTimerAtFixedRate(
      key: Any,
      msg: Any,
      initialDelay: java.time.Duration,
      interval: java.time.Duration): Unit =
    startTimerAtFixedRate(key, msg, initialDelay.toScala, interval.toScala)

  /**
   * Deprecated API: See [[TimerScheduler#startTimerWithFixedDelay]] or [[TimerScheduler#startTimerAtFixedRate]].
   */
  @deprecated(
    "Use startTimerWithFixedDelay or startTimerAtFixedRate instead. This has the same semantics as " +
    "startTimerAtFixedRate, but startTimerWithFixedDelay is often preferred.",
    since = "2.6.0")
  def startPeriodicTimer(key: Any, msg: Any, interval: FiniteDuration): Unit

  /**
   * Deprecated API: See [[TimerScheduler#startTimerWithFixedDelay]] or [[TimerScheduler#startTimerAtFixedRate]].
   */
  @deprecated(
    "Use startTimerWithFixedDelay or startTimerAtFixedRate instead. This has the same semantics as " +
    "startTimerAtFixedRate, but startTimerWithFixedDelay is often preferred.",
    since = "2.6.0")
  final def startPeriodicTimer(key: Any, msg: Any, interval: java.time.Duration): Unit =
    startPeriodicTimer(key, msg, interval.toScala)

  /**
   * Start a timer that will send `msg` once to the `self` actor after
   * the given `timeout`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startSingleTimer(key: Any, msg: Any, timeout: FiniteDuration): Unit

  /**
   * Start a timer that will send `msg` once to the `self` actor after
   * the given `timeout`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  final def startSingleTimer(key: Any, msg: Any, timeout: java.time.Duration): Unit =
    startSingleTimer(key, msg, timeout.toScala)

  /**
   * Check if a timer with a given `key` is active.
   */
  def isTimerActive(key: Any): Boolean

  /**
   * Cancel a timer with a given `key`.
   * If canceling a timer that was already canceled, or key never was used to start a timer
   * this operation will do nothing.
   *
   * It is guaranteed that a message from a canceled timer, including its previous incarnation
   * for the same key, will not be received by the actor, even though the message might already
   * be enqueued in the mailbox when cancel is called.
   */
  def cancel(key: Any): Unit

  /**
   * Cancel all timers.
   */
  def cancelAll(): Unit

}
