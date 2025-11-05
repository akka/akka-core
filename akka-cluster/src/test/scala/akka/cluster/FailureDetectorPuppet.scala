/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster

import java.util.concurrent.atomic.AtomicReference

import com.typesafe.config.Config

import akka.event.EventStream
import akka.remote.FailureDetector
import akka.util.unused

/**
 * User controllable "puppet" failure detector.
 */
class FailureDetectorPuppet(@unused config: Config, @unused ev: EventStream) extends FailureDetector {

  sealed trait Status
  object Up extends Status
  object Down extends Status
  object Unknown extends Status

  private val status: AtomicReference[Status] = new AtomicReference(Unknown)

  def markNodeAsUnavailable(): Unit = status.set(Down)

  def markNodeAsAvailable(): Unit = status.set(Up)

  override def isAvailable: Boolean = status.get match {
    case Unknown | Up => true
    case Down         => false

  }

  override def isMonitoring: Boolean = status.get != Unknown

  override def heartbeat(): Unit = status.compareAndSet(Unknown, Up)

}
