/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import scala.concurrent.duration.FiniteDuration

import akka.annotation.DoNotInherit

/**
 * Support for scheduled `self` messages in an actor.
 * It is used with `Behaviors.withTimers`.
 * Timers are bound to the lifecycle of the actor that owns it,
 * and thus are cancelled automatically when it is restarted or stopped.
 *
 * `TimerScheduler` is not thread-safe, i.e. it must only be used within
 * the actor that owns it.
 *
 * Not for user extension.
 */
@DoNotInherit
trait TimerScheduler[T] {

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
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
   * previous timer is not received, even if it was already be enqueued
   * in the mailbox before the new timer was started.
   */
  def startTimerWithFixedDelay(key: Any, msg: T, delay: FiniteDuration): Unit

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
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
   * previous timer is not received, even if it was already be enqueued
   * in the mailbox before the new timer was started.
   */
  def startTimerWithFixedDelay(key: Any, msg: T, initialDelay: FiniteDuration, delay: FiniteDuration): Unit

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * When a new timer is started with the same message,
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started. If you do not want this,
   * you can start start them as individual timers by specifying distinct keys.
   */
  def startTimerWithFixedDelay(msg: T, delay: FiniteDuration): Unit =
    startTimerWithFixedDelay(msg, msg, delay)

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages after the `initialDelay`.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * When a new timer is started with the same message,
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started. If you do not want this,
   * you can start start them as individual timers by specifying distinct keys.
   */
  def startTimerWithFixedDelay(msg: T, initialDelay: FiniteDuration, delay: FiniteDuration): Unit =
    startTimerWithFixedDelay(msg, msg, initialDelay, delay)

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
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
  def startTimerAtFixedRate(key: Any, msg: T, interval: FiniteDuration): Unit

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
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
  def startTimerAtFixedRate(key: Any, msg: T, initialDelay: FiniteDuration, interval: FiniteDuration): Unit

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
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
   * When a new timer is started with the same message
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started. If you do not want this,
   * you can start start them as individual timers by specifying distinct keys.
   */
  def startTimerAtFixedRate(msg: T, interval: FiniteDuration): Unit =
    startTimerAtFixedRate(msg, msg, interval)

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
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
   * When a new timer is started with the same message
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started. If you do not want this,
   * you can start start them as individual timers by specifying distinct keys.
   */
  def startTimerAtFixedRate(msg: T, initialDelay: FiniteDuration, interval: FiniteDuration): Unit =
    startTimerAtFixedRate(msg, msg, initialDelay, interval)

  /**
   * Deprecated API: See [[TimerScheduler#startTimerWithFixedDelay]] or [[TimerScheduler#startTimerAtFixedRate]].
   */
  @deprecated(
    "Use startTimerWithFixedDelay or startTimerAtFixedRate instead. This has the same semantics as " +
    "startTimerAtFixedRate, but startTimerWithFixedDelay is often preferred.",
    since = "2.6.0")
  def startPeriodicTimer(key: Any, msg: T, interval: FiniteDuration): Unit

  /**
   * Start a timer that will send `msg` once to the `self` actor after
   * the given `delay`.
   *
   * Each timer has a key and if a new timer with same key is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startSingleTimer(key: Any, msg: T, delay: FiniteDuration): Unit

  /**
   * Start a timer that will send `msg` once to the `self` actor after
   * the given `delay`.
   *
   * If a new timer is started with the same message
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started. If you do not want this,
   * you can start start them as individual timers by specifying distinct keys.
   */
  def startSingleTimer(msg: T, delay: FiniteDuration): Unit =
    startSingleTimer(msg, msg, delay)

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
