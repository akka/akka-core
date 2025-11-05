/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import OverflowStrategies._

import akka.annotation.{ DoNotInherit, InternalApi }
import akka.event.Logging
import akka.event.Logging.LogLevel

/**
 * Represents a strategy that decides how to deal with a buffer of time based operator
 * that is full but is about to receive a new element.
 */
@DoNotInherit
sealed abstract class DelayOverflowStrategy extends Serializable {

  /** INTERNAL API */
  @InternalApi private[akka] def isBackpressure: Boolean
}

final case class BufferOverflowException(msg: String) extends RuntimeException(msg)

/**
 * Represents a strategy that decides how to deal with a buffer that is full but is
 * about to receive a new element.
 */
@DoNotInherit
sealed abstract class OverflowStrategy extends DelayOverflowStrategy {

  /** INTERNAL API */
  @InternalApi private[akka] def logLevel: LogLevel
  def withLogLevel(logLevel: Logging.LogLevel): OverflowStrategy
}

private[akka] object OverflowStrategies {

  /**
   * INTERNAL API
   */
  private[akka] case class DropHead(logLevel: LogLevel) extends OverflowStrategy {
    override def withLogLevel(logLevel: LogLevel): DropHead = DropHead(logLevel)
    private[akka] override def isBackpressure: Boolean = false
  }

  /**
   * INTERNAL API
   */
  private[akka] case class DropTail(logLevel: LogLevel) extends OverflowStrategy {
    override def withLogLevel(logLevel: LogLevel): DropTail = DropTail(logLevel)
    private[akka] override def isBackpressure: Boolean = false
  }

  /**
   * INTERNAL API
   */
  private[akka] case class DropBuffer(logLevel: LogLevel) extends OverflowStrategy {
    override def withLogLevel(logLevel: LogLevel): DropBuffer = DropBuffer(logLevel)
    private[akka] override def isBackpressure: Boolean = false
  }

  /**
   * INTERNAL API
   */
  private[akka] case class DropNew(logLevel: LogLevel) extends OverflowStrategy {
    override def withLogLevel(logLevel: LogLevel): DropNew = DropNew(logLevel)
    private[akka] override def isBackpressure: Boolean = false
  }

  /**
   * INTERNAL API
   */
  private[akka] case class Backpressure(logLevel: LogLevel) extends OverflowStrategy {
    override def withLogLevel(logLevel: LogLevel): Backpressure = Backpressure(logLevel)
    private[akka] override def isBackpressure: Boolean = true
  }

  /**
   * INTERNAL API
   */
  private[akka] case class Fail(logLevel: LogLevel) extends OverflowStrategy {
    override def withLogLevel(logLevel: LogLevel): Fail = Fail(logLevel)
    private[akka] override def isBackpressure: Boolean = false
  }

  /**
   * INTERNAL API
   */
  private[akka] case object EmitEarly extends DelayOverflowStrategy {
    private[akka] override def isBackpressure: Boolean = true
  }
}

object OverflowStrategy {

  /**
   * If the buffer is full when a new element arrives, drops the oldest element from the buffer to make space for
   * the new element.
   */
  def dropHead: OverflowStrategy = DropHead(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element arrives, drops the youngest element from the buffer to make space for
   * the new element.
   */
  def dropTail: OverflowStrategy = DropTail(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element arrives, drops all the buffered elements to make space for the new element.
   */
  def dropBuffer: OverflowStrategy = DropBuffer(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element arrives, drops the new element.
   *
   * @deprecated Use {@link akka.stream.javadsl.Source#queue(int,akka.stream.OverflowStrategy)} instead
   */
  @deprecated("Use Source.queue instead", "2.6.11")
  @Deprecated
  def dropNew: OverflowStrategy = DropNew(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element is available this strategy backpressures the upstream publisher until
   * space becomes available in the buffer.
   */
  def backpressure: OverflowStrategy = Backpressure(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element is available this strategy completes the stream with failure.
   */
  def fail: OverflowStrategy = Fail(Logging.ErrorLevel)
}

object DelayOverflowStrategy {

  /**
   * If the buffer is full when a new element is available this strategy send next element downstream without waiting
   * Will backpressure if downstream is not ready.
   */
  def emitEarly: DelayOverflowStrategy = EmitEarly

  /**
   * If the buffer is full when a new element arrives, drops the oldest element from the buffer to make space for
   * the new element.
   */
  def dropHead: DelayOverflowStrategy = DropHead(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element arrives, drops the youngest element from the buffer to make space for
   * the new element.
   */
  def dropTail: DelayOverflowStrategy = DropTail(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element arrives, drops all the buffered elements to make space for the new element.
   */
  def dropBuffer: DelayOverflowStrategy = DropBuffer(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element arrives, drops the new element.
   */
  def dropNew: DelayOverflowStrategy = DropNew(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element is available this strategy backpressures the upstream publisher until
   * space becomes available in the buffer.
   */
  def backpressure: DelayOverflowStrategy = Backpressure(Logging.DebugLevel)

  /**
   * If the buffer is full when a new element is available this strategy completes the stream with failure.
   */
  def fail: DelayOverflowStrategy = Fail(Logging.ErrorLevel)
}
