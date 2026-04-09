/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import java.nio.ByteBuffer
import javax.net.ssl._
import javax.net.ssl.SSLEngineResult.HandshakeStatus
import javax.net.ssl.SSLEngineResult.HandshakeStatus._
import javax.net.ssl.SSLEngineResult.Status._

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.TLSProtocol._
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic }
import akka.util.ByteString

/**
 * INTERNAL API.
 *
 * GraphStage implementation of the TLS bidi. Replaces the legacy
 * [[TlsModule]] + `TLSActor` pair. Ports the SSLEngine wrap/unwrap state
 * machine one-for-one from the previous actor implementation, preserving
 * all JDK SSLEngine workarounds and `TLSClosing` semantics.
 */
@InternalApi private[stream] final class TlsStage(
    createSSLEngine: () => SSLEngine,
    verifySession: SSLSession => Try[Unit],
    closing: TLSClosing)
    extends GraphStage[BidiShape[SslTlsOutbound, ByteString, ByteString, SslTlsInbound]] {

  val plainIn: Inlet[SslTlsOutbound] = Inlet("StreamTls.transportIn")
  val plainOut: Outlet[SslTlsInbound] = Outlet("StreamTls.transportOut")
  val cipherIn: Inlet[ByteString] = Inlet("StreamTls.cipherIn")
  val cipherOut: Outlet[ByteString] = Outlet("StreamTls.cipherOut")

  override val shape: BidiShape[SslTlsOutbound, ByteString, ByteString, SslTlsInbound] =
    BidiShape(plainIn, cipherOut, cipherIn, plainOut)

  override def initialAttributes: Attributes = Attributes.name("StreamTls")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TlsStageLogic(shape, createSSLEngine, verifySession, closing)
}

/** INTERNAL API */
@InternalApi private[stream] object TlsStage {
  sealed trait Phase
  case object Bidirectional extends Phase
  case object FlushingOutbound extends Phase
  case object AwaitingClose extends Phase
  case object OutboundClosed extends Phase
  case object InboundClosed extends Phase
  case object CompletedPhase extends Phase
}

/** INTERNAL API */
@InternalApi
private[stream] final class TlsStageLogic(
    shape: BidiShape[SslTlsOutbound, ByteString, ByteString, SslTlsInbound],
    createSSLEngine: () => SSLEngine,
    verifySession: SSLSession => Try[Unit],
    closing: TLSClosing)
    extends TimerGraphStageLogic(shape) {

  import TlsStage._

  private def plainIn: Inlet[SslTlsOutbound] = shape.in1
  private def cipherOut: Outlet[ByteString] = shape.out1
  private def cipherIn: Inlet[ByteString] = shape.in2
  private def plainOut: Outlet[SslTlsInbound] = shape.out2

  override def toString: String = "StreamTls"

  // SSLEngine needs bite-sized chunks. Each ChoppingBlock holds left-over
  // bytes and serves them into an SSLEngine-sized ByteBuffer.
  private final class ChoppingBlock {
    private var buffer: ByteString = ByteString.empty

    def hasData: Boolean = buffer.nonEmpty
    def isEmpty: Boolean = buffer.isEmpty
    def size: Int = buffer.size

    def offer(bs: ByteString): Unit =
      if (bs.nonEmpty) buffer = if (buffer.isEmpty) bs else buffer ++ bs

    def chopInto(b: ByteBuffer): Unit = {
      b.compact()
      val copied = buffer.copyToBuffer(b)
      buffer = buffer.drop(copied)
      b.flip()
    }

    def putBack(b: ByteBuffer): Unit =
      if (b.hasRemaining) {
        val bs = ByteString(b)
        if (bs.nonEmpty) buffer = bs ++ buffer
        prepare(b)
      }

    def prepare(b: ByteBuffer): Unit = {
      b.clear()
      b.limit(0)
    }
  }

  // Netty's default sizes: 16665 + 1024 (compressed data) + 1024 (OpenJDK compat).
  private val transportOutBuffer = ByteBuffer.allocate(16665 + 2048)
  // Larger: chopping multiple input packets can lead to OVERFLOW that is also
  // UNDERFLOW; size for two packets to avoid extra copying.
  private val userOutBuffer = ByteBuffer.allocate(16665 * 2 + 2048)
  private val transportInBuffer = ByteBuffer.allocate(16665 + 2048)
  private val userInBuffer = ByteBuffer.allocate(16665 + 2048)

  private val userInChoppingBlock = new ChoppingBlock
  private val transportInChoppingBlock = new ChoppingBlock

  // Pending user-side outbound elements awaiting processing. NegotiateNewSession
  // must be delayed until the preceding SendBytes have been wrapped, otherwise
  // the engine would still be completing the initial handshake when we ask it
  // to renegotiate, and JSSE silently ignores a second beginHandshake while
  // already handshaking.
  private val userInQueue = new java.util.ArrayDeque[SslTlsOutbound]()

  private var engine: SSLEngine = _
  private var currentSession: SSLSession = _
  private var lastHandshakeStatus: HandshakeStatus = _
  private var corkUser: Boolean = true
  private var unwrapPutBackCounter: Int = 0

  private var phase: Phase = Bidirectional
  private var pumping: Boolean = false

  // The very first pump triggered by downstream demand (cipherOut/plainOut
  // onPull) is deferred to the next scheduler tick so any inlet activity
  // queued during materialization (notably a Source.failed wired to an inlet)
  // gets a chance to short-circuit the stage before we speculatively produce
  // handshake bytes for the peer.
  private var warmupDone: Boolean = false
  private object InitialPumpTimer

  // Pending SessionTruncated emission to plainOut; delivered before completion.
  private var pendingTruncated: Boolean = false
  // Plaintext output buffered when plainOut had no demand at the moment of
  // unwrap. The TLSActor predecessor relied on OutputBunch's internal buffer;
  // we emulate that with a single-element slot that must be drained before
  // the next unwrap runs.
  private var pendingUserOut: SessionBytes = null

  override def preStart(): Unit = {
    userInChoppingBlock.prepare(userInBuffer)
    transportInChoppingBlock.prepare(transportInBuffer)

    try {
      engine = createSSLEngine()
      engine.beginHandshake()
      lastHandshakeStatus = engine.getHandshakeStatus
      currentSession = engine.getSession
    } catch {
      case NonFatal(ex) =>
        failStage(ex)
        throw ex
    }

    pull(plainIn)
    pull(cipherIn)
  }

  // ----- Handlers -----

  private def finishWarmup(): Unit =
    if (!warmupDone) {
      warmupDone = true
      cancelTimer(InitialPumpTimer)
    }

  setHandler(
    plainIn,
    new InHandler {
      override def onPush(): Unit = {
        userInQueue.add(grab(plainIn))
        finishWarmup()
        pump()
      }

      override def onUpstreamFinish(): Unit = {
        finishWarmup()
        pump()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        finishWarmup()
        failTls(ex, closeTransport = true)
      }
    })

  setHandler(
    cipherIn,
    new InHandler {
      override def onPush(): Unit = {
        transportInChoppingBlock.offer(grab(cipherIn))
        finishWarmup()
        pump()
      }

      override def onUpstreamFinish(): Unit = {
        finishWarmup()
        pump()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        finishWarmup()
        failTls(ex, closeTransport = true)
      }
    })

  private def maybeDeferredPump(): Unit = {
    if (warmupDone) pump()
    else if (!isTimerActive(InitialPumpTimer))
      scheduleOnce(InitialPumpTimer, Duration.Zero)
  }

  override protected def onTimer(timerKey: Any): Unit =
    if (timerKey == InitialPumpTimer) {
      warmupDone = true
      pump()
    }

  setHandler(cipherOut, new OutHandler {
    override def onPull(): Unit = maybeDeferredPump()

    override def onDownstreamFinish(cause: Throwable): Unit = {
      // Transport is gone; nothing more we can do.
      phase = CompletedPhase
      completeStage()
    }
  })

  setHandler(plainOut, new OutHandler {
    override def onPull(): Unit = maybeDeferredPump()

    override def onDownstreamFinish(cause: Throwable): Unit = pump()
  })

  // ----- Readiness predicates (pure queries) -----

  private def userHasData: Boolean =
    !corkUser && (userInChoppingBlock.hasData || userHasQueuedSendBytes) && lastHandshakeStatus != NEED_UNWRAP
  // True only if the next pending user element is actual plaintext bytes
  // (not a NegotiateNewSession, which must not drive outbound progress while
  // a handshake is pending).
  private def userHasQueuedSendBytes: Boolean = {
    val head = userInQueue.peek()
    head != null && head.isInstanceOf[SendBytes]
  }
  private def userInAtEnd: Boolean =
    isClosed(plainIn) && userInChoppingBlock.isEmpty && userInQueue.isEmpty
  private def transportHasData: Boolean = transportInChoppingBlock.hasData || isAvailable(cipherIn)
  private def transportInAtEnd: Boolean = isClosed(cipherIn) && transportInChoppingBlock.isEmpty
  private def engineNeedsWrap: Boolean = lastHandshakeStatus == NEED_WRAP && !engine.isOutboundDone
  private def engineInboundOpen: Boolean = !engine.isInboundDone
  private def userOutCancelled: Boolean = isClosed(plainOut)

  // Inbound progress is allowed even without plainOut demand (handshake
  // bytes don't surface to the user), as long as there's no undelivered
  // pending user output to flush first.
  private def canUnwrap: Boolean = pendingUserOut == null
  private def outboundReady: Boolean =
    (userHasData || engineNeedsWrap) && isAvailable(cipherOut)
  private def inboundReady: Boolean =
    (transportHasData && canUnwrap) || userOutCancelled
  private def outboundHalfClosedReady: Boolean =
    engineNeedsWrap && isAvailable(cipherOut)
  private def inboundHalfClosedReady: Boolean =
    transportHasData && engineInboundOpen && canUnwrap

  // ----- Pump loop -----

  private def pump(): Unit = {
    if (pumping) return
    pumping = true
    try {
      var madeProgress = true
      while (madeProgress && (phase ne CompletedPhase)) {
        val phaseBefore = phase
        val hsBefore = lastHandshakeStatus
        val cOutAvailBefore = isAvailable(cipherOut)
        val pOutAvailBefore = isAvailable(plainOut)
        val transChopBefore = transportInChoppingBlock.size
        val userChopBefore = userInChoppingBlock.size
        val transBufPosBefore = transportInBuffer.position()

        // Drain pending plaintext / SessionTruncated to plainOut first.
        if (pendingUserOut != null && isAvailable(plainOut)) {
          push(plainOut, pendingUserOut)
          pendingUserOut = null
        }
        if (pendingTruncated && isAvailable(plainOut)) {
          push(plainOut, SessionTruncated)
          pendingTruncated = false
        }

        // Surface any renegotiation that's now due (engine idle + nothing
        // left to wrap with the current cipher).
        if (userInChoppingBlock.isEmpty) drainUserInQueueIntoChop()

        phase match {
          case Bidirectional =>
            if (outboundReady || inboundReady || userInAtEnd) {
              val continue = doInbound(isOutboundClosed = false, inboundHalfClosed = false)
              if (continue && (phase eq Bidirectional))
                doOutbound(isInboundClosed = false)
            }

          case FlushingOutbound =>
            if (outboundHalfClosedReady) {
              try doWrap()
              catch { case _: SSLException => nextPhase(CompletedPhase) }
            }

          case AwaitingClose =>
            if (transportHasData && engineInboundOpen) {
              ensureTransportInChopped()
              try doUnwrap(ignoreOutput = true)
              catch { case _: SSLException => nextPhase(CompletedPhase) }
            } else if (transportInAtEnd) {
              nextPhase(CompletedPhase)
            }

          case OutboundClosed =>
            if (outboundHalfClosedReady || inboundReady) {
              val continue = doInbound(isOutboundClosed = true, inboundHalfClosed = false)
              if (continue && (phase eq OutboundClosed) && outboundHalfClosedReady) {
                try doWrap()
                catch { case _: SSLException => nextPhase(CompletedPhase) }
              }
            }

          case InboundClosed =>
            if (outboundReady || inboundHalfClosedReady || userInAtEnd) {
              val continue = doInbound(isOutboundClosed = false, inboundHalfClosed = true)
              if (continue && (phase eq InboundClosed))
                doOutbound(isInboundClosed = true)
            }

          case CompletedPhase =>
        }

        // Real progress: phase changed, engine handshake status advanced, a
        // push consumed downstream demand, or wrap/unwrap touched buffer state.
        madeProgress = (phase ne phaseBefore) ||
          (lastHandshakeStatus ne hsBefore) ||
          (cOutAvailBefore && !isAvailable(cipherOut)) ||
          (pOutAvailBefore && !isAvailable(plainOut)) ||
          (transportInChoppingBlock.size != transChopBefore) ||
          (userInChoppingBlock.size != userChopBefore) ||
          (transportInBuffer.position() != transBufPosBefore)
      }

      if (phase eq CompletedPhase) {
        if (pendingTruncated && isAvailable(plainOut)) {
          push(plainOut, SessionTruncated)
          pendingTruncated = false
        }
        if (!pendingTruncated) completeStage()
      } else {
        // Demand-driven pulling: keep inlets primed when we still need bytes.
        // Pull only when both the chopping block and the queue are empty so
        // we don't accumulate more than one element ahead.
        if (!userInChoppingBlock.hasData && userInQueue.isEmpty &&
            !hasBeenPulled(plainIn) && !isClosed(plainIn)) pull(plainIn)
        if (!transportInChoppingBlock.hasData && !hasBeenPulled(cipherIn) && !isClosed(cipherIn)) pull(cipherIn)
      }
    } finally {
      pumping = false
    }
  }

  private def nextPhase(p: Phase): Unit = phase = p

  private def ensureTransportInChopped(): Unit = {
    if (transportInChoppingBlock.isEmpty && isAvailable(cipherIn))
      transportInChoppingBlock.offer(grab(cipherIn))
    transportInChoppingBlock.chopInto(transportInBuffer)
  }

  /**
   * Drain pending queued elements from the user side into the chopping block
   * until we hit a `NegotiateNewSession` or the queue is empty.
   *
   * Unlike SendBytes, a NegotiateNewSession needs the engine to be in a
   * NOT_HANDSHAKING/FINISHED state to trigger a renegotiation; calling
   * beginHandshake while a handshake is already in progress is a no-op in
   * JSSE. Therefore we only apply it once all previously buffered plaintext
   * has been wrapped AND the engine is idle.
   */
  private def drainUserInQueueIntoChop(): Unit = {
    var done = false
    while (!done && !userInQueue.isEmpty && userInChoppingBlock.isEmpty) {
      userInQueue.peek() match {
        case SendBytes(bs) =>
          userInQueue.poll()
          userInChoppingBlock.offer(bs)
        case n: NegotiateNewSession =>
          if (lastHandshakeStatus == HandshakeStatus.NOT_HANDSHAKING ||
              lastHandshakeStatus == HandshakeStatus.FINISHED) {
            userInQueue.poll()
            setNewSessionParameters(n)
          }
          done = true
      }
    }
  }

  private def ensureUserInChopped(): Unit = {
    if (userInChoppingBlock.isEmpty) drainUserInQueueIntoChop()
    userInChoppingBlock.chopInto(userInBuffer)
  }

  // ----- Phase bodies -----

  private def doInbound(isOutboundClosed: Boolean, inboundHalfClosed: Boolean): Boolean = {
    if (transportInAtEnd) {
      try engine.closeInbound()
      catch { case _: SSLException => pendingTruncated = true }
      lastHandshakeStatus = engine.getHandshakeStatus
      completeOrFlush()
      false
    } else if (!inboundHalfClosed && userOutCancelled) {
      if (!isOutboundClosed && closing.ignoreCancel) {
        nextPhase(InboundClosed)
      } else {
        engine.closeOutbound()
        lastHandshakeStatus = engine.getHandshakeStatus
        nextPhase(FlushingOutbound)
      }
      true
    } else if (if (inboundHalfClosed) inboundHalfClosedReady
               else transportHasData && canUnwrap) {
      ensureTransportInChopped()
      try {
        doUnwrap(ignoreOutput = false)
        true
      } catch {
        case ex: SSLException =>
          failTls(ex, closeTransport = false)
          try engine.closeInbound()
          catch { case _: SSLException => () }
          completeOrFlush()
          false
      }
    } else true
  }

  private def doOutbound(isInboundClosed: Boolean): Unit = {
    if (userInAtEnd && mayCloseOutbound) {
      if (isInboundClosed || !closing.ignoreComplete) {
        engine.closeOutbound()
        lastHandshakeStatus = engine.getHandshakeStatus
      }
      nextPhase(OutboundClosed)
    } else if (isClosed(cipherOut)) {
      nextPhase(CompletedPhase)
    } else if (outboundReady) {
      if (userHasData) ensureUserInChopped()
      try doWrap()
      catch {
        case ex: SSLException =>
          // Try to extract an alert for the peer before giving up.
          try {
            val empty = ByteBuffer.allocate(0)
            engine.wrap(empty, transportOutBuffer)
          } catch { case _: Throwable => () }
          flushToTransport()
          failTls(ex, closeTransport = false)
          completeOrFlush()
      }
    }
  }

  /**
   * In JDK 8 it is not allowed to call `closeOutbound` before the handshake is
   * done or otherwise an IllegalStateException might be thrown when the next
   * handshake packet arrives.
   */
  private def mayCloseOutbound: Boolean = lastHandshakeStatus match {
    case HandshakeStatus.NOT_HANDSHAKING | HandshakeStatus.FINISHED => true
    case _                                                          => false
  }

  private def completeOrFlush(): Unit =
    if (engine.isOutboundDone || (engine.isInboundDone && userInChoppingBlock.isEmpty))
      nextPhase(CompletedPhase)
    else
      nextPhase(FlushingOutbound)

  private def flushToTransport(): Unit = {
    transportOutBuffer.flip()
    if (transportOutBuffer.hasRemaining && isAvailable(cipherOut))
      push(cipherOut, ByteString(transportOutBuffer))
    transportOutBuffer.clear()
  }

  private def flushToUser(): Unit = {
    if (unwrapPutBackCounter > 0) unwrapPutBackCounter = 0
    userOutBuffer.flip()
    if (userOutBuffer.hasRemaining) {
      if (!isClosed(plainOut)) {
        val elem = SessionBytes(currentSession, ByteString(userOutBuffer))
        if (isAvailable(plainOut)) push(plainOut, elem)
        else pendingUserOut = elem
      }
      // else drop: plainOut is cancelled (InboundClosed phase)
    }
    userOutBuffer.clear()
  }

  private def doWrap(): Unit = {
    val result = engine.wrap(userInBuffer, transportOutBuffer)
    lastHandshakeStatus = result.getHandshakeStatus

    if (lastHandshakeStatus == FINISHED) handshakeFinished()
    runDelegatedTasks()
    result.getStatus match {
      case OK =>
        // https://github.com/akka/akka-core/issues/29922
        // Guard against JDK SSLEngine getting stuck in OK+NEED_WRAP with no output.
        if (transportOutBuffer.position() == 0 && lastHandshakeStatus == NEED_WRAP)
          throw new IllegalStateException("SSLEngine trying to loop NEED_WRAP without producing output")

        flushToTransport()
        userInChoppingBlock.putBack(userInBuffer)
      case CLOSED =>
        flushToTransport()
        if (engine.isInboundDone) nextPhase(CompletedPhase)
        else nextPhase(AwaitingClose)
      case s => failTls(new IllegalStateException(s"unexpected status $s in doWrap()"))
    }
  }

  @tailrec
  private def doUnwrap(ignoreOutput: Boolean): Unit = {
    val oldInPosition = transportInBuffer.position()
    val result = engine.unwrap(transportInBuffer, userOutBuffer)
    if (ignoreOutput) userOutBuffer.clear()
    lastHandshakeStatus = result.getHandshakeStatus
    runDelegatedTasks()
    result.getStatus match {
      case OK =>
        result.getHandshakeStatus match {
          case NEED_WRAP =>
            // https://github.com/akka/akka-core/issues/29922
            unwrapPutBackCounter += 1
            if (unwrapPutBackCounter > 1000) {
              throw new IllegalStateException(
                s"Stuck in unwrap loop, bailing out, last handshake status [$lastHandshakeStatus], " +
                s"remaining=${transportInBuffer.remaining}, out=${userOutBuffer.position()}, " +
                "(https://github.com/akka/akka-core/issues/29922)")
            }
            transportInChoppingBlock.putBack(transportInBuffer)
          case FINISHED =>
            flushToUser()
            handshakeFinished()
            transportInChoppingBlock.putBack(transportInBuffer)
          case NEED_UNWRAP
              if transportInBuffer.hasRemaining &&
              userOutBuffer.position() == 0 &&
              transportInBuffer.position() == oldInPosition =>
            throw new IllegalStateException("SSLEngine trying to loop NEED_UNWRAP without producing output")
          case _ =>
            if (transportInBuffer.hasRemaining) doUnwrap(ignoreOutput = false)
            else flushToUser()
        }
      case CLOSED =>
        flushToUser()
        completeOrFlush()
      case BUFFER_UNDERFLOW =>
        flushToUser()
      case BUFFER_OVERFLOW =>
        flushToUser()
        transportInChoppingBlock.putBack(transportInBuffer)
      case null => failTls(new IllegalStateException(s"unexpected status 'null' in doUnwrap()"))
    }
  }

  // SSLEngine delegated tasks (cert validation, etc.) are blocking but match
  // the previous TLSActor behaviour by running them inline on the interpreter
  // thread. TODO: consider offloading to a separate dispatcher.
  @tailrec
  private def runDelegatedTasks(): Unit = {
    val task = engine.getDelegatedTask
    if (task != null) {
      task.run()
      runDelegatedTasks()
    } else {
      lastHandshakeStatus = engine.getHandshakeStatus
    }
  }

  private def handshakeFinished(): Unit = {
    val session = engine.getSession
    verifySession(session) match {
      case Success(()) =>
        currentSession = session
        corkUser = false
        flushToUser()
      case Failure(ex) =>
        failTls(ex, closeTransport = true)
    }
  }

  private def setNewSessionParameters(params: NegotiateNewSession): Unit = {
    currentSession.invalidate()
    TlsUtils.applySessionParameters(engine, params)
    engine.beginHandshake()
    lastHandshakeStatus = engine.getHandshakeStatus
    corkUser = true
  }

  private def failTls(ex: Throwable): Unit = failTls(ex, closeTransport = true)

  private def failTls(e: Throwable, closeTransport: Boolean): Unit =
    if (closeTransport) {
      nextPhase(CompletedPhase)
      failStage(e)
    } else {
      // Asymmetric failure: only fail the user side and stop accepting more
      // user input. cipherOut may still be needed to flush a TLS alert.
      if (!isClosed(plainOut)) fail(plainOut, e)
      if (!isClosed(plainIn)) cancel(plainIn)
    }
}
