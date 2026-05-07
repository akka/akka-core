/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.actor.{ Actor, ActorLogging, Props }
import akka.annotation.InternalApi
import akka.remote.UniqueAddress

/**
 * INTERNAL API
 */
@InternalApi
private[remote] object FlushOnShutdown {
  def props(done: Promise[Done], timeout: FiniteDuration, associations: Set[Association]): Props = {
    require(associations.nonEmpty)
    Props(new FlushOnShutdown(done, timeout, associations))
  }

  private case object Timeout
}

/**
 * INTERNAL API
 */
@InternalApi
private[remote] class FlushOnShutdown(done: Promise[Done], timeout: FiniteDuration, associations: Set[Association])
    extends Actor
    with ActorLogging {

  var remaining = Map.empty[UniqueAddress, Int]

  private val timeoutTask =
    context.system.scheduler.scheduleOnce(timeout, self, FlushOnShutdown.Timeout)(context.dispatcher)

  override def preStart(): Unit = {
    try {
      associations.foreach { a =>
        val acksExpected = a.sendTerminationHint(self)
        a.associationState.uniqueRemoteAddress() match {
          case Some(address) => remaining += address -> acksExpected
          case None          => // Ignore, handshake was not completed on this association
        }
      }
      if (remaining.valuesIterator.sum == 0) {
        done.trySuccess(Done)
        context.stop(self)
      }
    } catch {
      case NonFatal(e) =>
        // sendTerminationHint may throw
        done.tryFailure(e)
        throw e
    }
  }

  override def postStop(): Unit = {
    timeoutTask.cancel()
    done.trySuccess(Done)
    log.debug("FlushOnShutdown stopped")
  }

  def receive: Receive = {
    case ActorSystemTerminatingAck(from) =>
      log.debug("ActorSystemTerminatingAck from [{}]", from)
      // Just treat unexpected acks as systems from which zero acks are expected
      val acksRemaining = remaining.getOrElse(from, 0)
      if (acksRemaining <= 1) {
        remaining -= from
      } else {
        remaining = remaining.updated(from, acksRemaining - 1)
      }

      if (remaining.isEmpty) {
        log.debug("Flushing completed")
        context.stop(self)
      }
    case FlushOnShutdown.Timeout =>
      log.debug(
        "Flush of remote transport timed out after [{}]. Remaining [{}] associations.",
        timeout.toCoarsest,
        remaining.size)
      context.stop(self)
  }
}
