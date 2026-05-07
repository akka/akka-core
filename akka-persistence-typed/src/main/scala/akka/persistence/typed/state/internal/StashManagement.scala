/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import akka.actor.Dropped
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.actor.typed.scaladsl.StashOverflowException
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.util.ConstantFun

/** INTERNAL API: Stash management for persistent behaviors */
@InternalApi
private[akka] trait StashManagement[C, S] {

  def setup: BehaviorSetup[C, S]

  private def context: ActorContext[InternalProtocol] = setup.context

  private def stashState: StashState = setup.stashState

  protected def isInternalStashEmpty: Boolean = stashState.internalStashBuffer.isEmpty

  /**
   * Stash a command to the internal stash buffer, which is used while waiting for persist to be completed.
   */
  protected def stashInternal(msg: InternalProtocol): Behavior[InternalProtocol] = {
    stash(msg, stashState.internalStashBuffer)
    Behaviors.same
  }

  /**
   * Stash a command to the user stash buffer, which is used when `Stash` effect is used.
   */
  protected def stashUser(msg: InternalProtocol): Unit =
    stash(msg, stashState.userStashBuffer)

  private def stash(msg: InternalProtocol, buffer: StashBuffer[InternalProtocol]): Unit = {
    logStashMessage(msg, buffer)

    try buffer.stash(msg)
    catch {
      case e: StashOverflowException =>
        setup.settings.stashOverflowStrategy match {
          case StashOverflowStrategy.Drop =>
            val dropName = msg match {
              case InternalProtocol.IncomingCommand(actual) => actual.getClass.getName
              case other                                    => other.getClass.getName
            }
            context.log.warn("Stash buffer is full, dropping message [{}]", dropName)
            context.system.toClassic.eventStream.publish(Dropped(msg, "Stash buffer is full", context.self.toClassic))
          case StashOverflowStrategy.Fail =>
            throw e
        }
    }
  }

  /**
   * `tryUnstashOne` is called at the end of processing each command, or when persist is completed
   */
  protected def tryUnstashOne(behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    val buffer =
      if (stashState.isUnstashAllInProgress) stashState.userStashBuffer
      else stashState.internalStashBuffer

    if (buffer.nonEmpty) {
      logUnstashMessage(buffer)

      stashState.decrementUnstashAllProgress()

      buffer.unstash(behavior, 1, ConstantFun.scalaIdentityFunction)
    } else behavior

  }

  /**
   * @return false if `tryUnstashOne` will unstash a message
   */
  protected def isStashEmpty: Boolean =
    if (stashState.isUnstashAllInProgress) stashState.userStashBuffer.isEmpty
    else stashState.internalStashBuffer.isEmpty

  /**
   * Subsequent `tryUnstashOne` will drain the user stash buffer before using the
   * internal stash buffer. It will unstash as many commands as are in the buffer when
   * `unstashAll` was called, i.e. if subsequent commands stash more, those will
   * not be unstashed until `unstashAll` is called again.
   */
  protected def unstashAll(): Unit = {
    if (stashState.userStashBuffer.nonEmpty) {
      logUnstashAll()
      stashState.startUnstashAll()
      // tryUnstashOne is called from Running at the end of processing each command
      // or when persist is completed
    }
  }

  protected def isUnstashAllInProgress: Boolean =
    stashState.isUnstashAllInProgress

  private def logStashMessage(msg: InternalProtocol, buffer: StashBuffer[InternalProtocol]): Unit = {
    if (setup.settings.logOnStashing)
      setup.internalLogger.debug(
        "Stashing message to {} stash: [{}] ",
        if (buffer eq stashState.internalStashBuffer) "internal" else "user",
        msg)
  }

  private def logUnstashMessage(buffer: StashBuffer[InternalProtocol]): Unit = {
    if (setup.settings.logOnStashing)
      setup.internalLogger.debug(
        "Unstashing message from {} stash: [{}]",
        if (buffer eq stashState.internalStashBuffer) "internal" else "user",
        buffer.head)
  }

  private def logUnstashAll(): Unit = {
    if (setup.settings.logOnStashing)
      setup.internalLogger.debug(
        "Unstashing all [{}] messages from user stash, first is: [{}]",
        stashState.userStashBuffer.size,
        stashState.userStashBuffer.head)
  }

}

/** INTERNAL API: stash buffer state in order to survive restart of internal behavior */
@InternalApi
private[akka] class StashState(ctx: ActorContext[InternalProtocol], settings: DurableStateSettings) {

  private var _internalStashBuffer: StashBuffer[InternalProtocol] = StashBuffer(ctx, settings.stashCapacity)
  private var _userStashBuffer: StashBuffer[InternalProtocol] = StashBuffer(ctx, settings.stashCapacity)
  private var unstashAllInProgress = 0

  def internalStashBuffer: StashBuffer[InternalProtocol] = _internalStashBuffer

  def userStashBuffer: StashBuffer[InternalProtocol] = _userStashBuffer

  def clearStashBuffers(): Unit = {
    _internalStashBuffer = StashBuffer(ctx, settings.stashCapacity)
    _userStashBuffer = StashBuffer(ctx, settings.stashCapacity)
    unstashAllInProgress = 0
  }

  def isUnstashAllInProgress: Boolean =
    unstashAllInProgress > 0

  def decrementUnstashAllProgress(): Unit = {
    if (isUnstashAllInProgress)
      unstashAllInProgress -= 1
  }

  def startUnstashAll(): Unit =
    unstashAllInProgress = _userStashBuffer.size

}
