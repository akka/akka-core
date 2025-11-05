/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

import akka.actor.Address
import akka.event.Logging
import akka.remote.artery.OutboundHandshake.HandshakeReq
import akka.remote.testkit.Direction
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage._
import akka.util.OptionVal

object TestManagementCommands {

  /** INTERNAL API */
  @SerialVersionUID(1L)
  final case class FailInboundStreamOnce(ex: Throwable)
}

/**
 * INTERNAL API: Thread safe mutable state that is shared among
 * the test operators.
 */
private[remote] class SharedTestState {

  private val state = new AtomicReference[TestState](TestState(Map.empty, None))

  def anyBlackholePresent(): Boolean = state.get.blackholes.nonEmpty

  def isBlackhole(from: Address, to: Address): Boolean =
    state.get.blackholes.get(from) match {
      case Some(destinations) => destinations(to)
      case None               => false
    }

  /** Enable blackholing between given address in given direction */
  def blackhole(a: Address, b: Address, direction: Direction): Unit =
    direction match {
      case Direction.Send    => addBlackhole(a, b)
      case Direction.Receive => addBlackhole(b, a)
      case Direction.Both =>
        addBlackhole(a, b)
        addBlackhole(b, a)
    }

  /**
   * Cause the inbound stream to fail with the given exception.
   * Can be used to test inbound stream restart / recovery.
   */
  @tailrec final def failInboundStreamOnce(ex: Throwable): Unit = {
    val current = state.get
    if (state.compareAndSet(current, current.copy(failInboundStream = Some(ex)))) ()
    else failInboundStreamOnce(ex)
  }

  /**
   * Get the exception to fail the inbound stream with and immediately reset the state to not-failed.
   * This is used to simulate a single failure on the stream, where a successful restart recovers operations.
   */
  @tailrec final def getInboundFailureOnce: Option[Throwable] = {
    val current = state.get()
    if (state.compareAndSet(current, current.copy(failInboundStream = None))) current.failInboundStream
    else getInboundFailureOnce
  }

  @tailrec private def addBlackhole(from: Address, to: Address): Unit = {
    val current = state.get
    val newState = current.blackholes.get(from) match {
      case Some(destinations) => current.copy(blackholes = current.blackholes.updated(from, destinations + to))
      case None               => current.copy(blackholes = current.blackholes.updated(from, Set(to)))
    }
    if (!state.compareAndSet(current, newState))
      addBlackhole(from, to)
  }

  def passThrough(a: Address, b: Address, direction: Direction): Unit =
    direction match {
      case Direction.Send    => removeBlackhole(a, b)
      case Direction.Receive => removeBlackhole(b, a)
      case Direction.Both =>
        removeBlackhole(a, b)
        removeBlackhole(b, a)
    }

  @tailrec private def removeBlackhole(from: Address, to: Address): Unit = {
    val current = state.get
    val newState = current.blackholes.get(from) match {
      case Some(destinations) => current.copy(blackholes = current.blackholes.updated(from, destinations - to))
      case None               => current
    }
    if (!state.compareAndSet(current, newState))
      removeBlackhole(from, to)
  }

}

/**
 * INTERNAL API
 */
private[remote] final case class TestState(blackholes: Map[Address, Set[Address]], failInboundStream: Option[Throwable])

/**
 * INTERNAL API
 */
private[remote] class OutboundTestStage(outboundContext: OutboundContext, state: SharedTestState)
    extends GraphStage[FlowShape[OutboundEnvelope, OutboundEnvelope]] {
  val in: Inlet[OutboundEnvelope] = Inlet("OutboundTestStage.in")
  val out: Outlet[OutboundEnvelope] = Outlet("OutboundTestStage.out")
  override val shape: FlowShape[OutboundEnvelope, OutboundEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      // InHandler
      override def onPush(): Unit = {
        val env = grab(in)
        if (state.isBlackhole(outboundContext.localAddress.address, outboundContext.remoteAddress)) {
          log.debug(
            "dropping outbound message [{}] to [{}] because of blackhole",
            Logging.messageClassName(env.message),
            outboundContext.remoteAddress)
          pull(in) // drop message
        } else
          push(out, env)
      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }

}

/**
 * INTERNAL API
 */
private[remote] class InboundTestStage(inboundContext: InboundContext, state: SharedTestState)
    extends GraphStage[FlowShape[InboundEnvelope, InboundEnvelope]] {
  val in: Inlet[InboundEnvelope] = Inlet("InboundTestStage.in")
  val out: Outlet[InboundEnvelope] = Outlet("InboundTestStage.out")
  override val shape: FlowShape[InboundEnvelope, InboundEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      // InHandler
      override def onPush(): Unit = {
        state.getInboundFailureOnce match {
          case Some(shouldFailEx) =>
            log.info("Fail inbound stream from [{}]: {}", classOf[InboundTestStage].getName, shouldFailEx.getMessage)
            failStage(shouldFailEx)
          case _ =>
            val env = grab(in)
            env.association match {
              case OptionVal.Some(association) =>
                if (state.isBlackhole(inboundContext.localAddress.address, association.remoteAddress)) {
                  log.debug(
                    "dropping inbound message [{}] from [{}] with UID [{}] because of blackhole",
                    Logging.messageClassName(env.message),
                    association.remoteAddress,
                    env.originUid)
                  pull(in) // drop message
                } else
                  push(out, env)
              case _ =>
                // unknown, handshake not completed
                if (state.anyBlackholePresent()) {
                  env.message match {
                    case _: HandshakeReq =>
                      log.debug(
                        "inbound message [{}] before handshake completed, cannot check if remote is blackholed, letting through",
                        Logging.messageClassName(env.message))
                      push(out, env) // let it through

                    case anyOther =>
                      log.debug(
                        "dropping inbound message [{}] with UID [{}] because of blackhole",
                        Logging.messageClassName(anyOther),
                        env.originUid)
                      pull(in) // drop message
                  }
                } else {
                  push(out, env)
                }
            }
        }
      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }

}
