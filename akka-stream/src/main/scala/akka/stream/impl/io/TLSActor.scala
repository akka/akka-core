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
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import akka.actor._
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.TLSProtocol._
import akka.stream.impl._
import akka.stream.impl.FanIn.InputBunch
import akka.stream.impl.FanOut.OutputBunch
import akka.stream.impl.fusing.ActorGraphInterpreter
import akka.stream.snapshot.StreamSnapshotImpl
import akka.util.ByteString

/**
 * INTERNAL API.
 */
@InternalApi private[stream] object TLSActor {

  def props(
      maxInputBufferSize: Int,
      createSSLEngine: () => SSLEngine,
      verifySession: SSLSession => Try[Unit],
      closing: TLSClosing,
      tracing: Boolean = false): Props =
    Props(new TLSActor(maxInputBufferSize, createSSLEngine, verifySession, closing, tracing)).withDeploy(Deploy.local)

  final val TransportIn = 0
  final val TransportOut = 0

  final val UserOut = 1
  final val UserIn = 1
}

/**
 * INTERNAL API.
 */
@InternalApi private[stream] class TLSActor(
    maxInputBufferSize: Int,
    createSSLEngine: () => SSLEngine,
    verifySession: SSLSession => Try[Unit],
    closing: TLSClosing,
    tracing: Boolean)
    extends Actor
    with ActorLogging
    with Pump {

  import TLSActor._

  private var stopped = false
  private var unwrapPutBackCounter: Int = 0

  protected val outputBunch = new OutputBunch(outputCount = 2, self, this)
  outputBunch.markAllOutputs()

  protected val inputBunch = new InputBunch(inputCount = 2, maxInputBufferSize, this) {
    override def onError(input: Int, e: Throwable): Unit = fail(e)
  }

  /**
   * The SSLEngine needs bite-sized chunks of data but we get arbitrary ByteString
   * from both the UserIn and the TransportIn ports. This is used to chop up such
   * a ByteString by filling the respective ByteBuffer and taking care to dequeue
   * a new element when data are demanded and none are left lying on the chopping
   * block.
   */
  class ChoppingBlock(idx: Int, name: String) extends TransferState {
    override def isReady: Boolean = buffer.nonEmpty || inputBunch.isPending(idx) || inputBunch.isDepleted(idx)
    override def isCompleted: Boolean = inputBunch.isCancelled(idx)

    private var buffer = ByteString.empty

    /**
     * Whether there are no bytes lying on this chopping block.
     */
    def isEmpty: Boolean = buffer.isEmpty

    /**
     * Pour as many bytes as are available either on the chopping block or in
     * the inputBunch’s next ByteString into the supplied ByteBuffer, which is
     * expected to be in “read left-overs” mode, i.e. everything between its
     * position and limit is retained. In order to allocate a fresh ByteBuffer
     * with these characteristics, use `prepare()`.
     */
    def chopInto(b: ByteBuffer): Unit = {
      b.compact()
      if (buffer.isEmpty) {
        buffer = inputBunch.dequeue(idx) match {
          // this class handles both UserIn and TransportIn
          case bs: ByteString => bs
          case SendBytes(bs)  => bs
          case n: NegotiateNewSession =>
            setNewSessionParameters(n)
            ByteString.empty
          case _ => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
        }
        if (tracing) log.debug(s"chopping from new chunk of ${buffer.size} into $name (${b.position()})")
      } else {
        if (tracing) log.debug(s"chopping from old chunk of ${buffer.size} into $name (${b.position()})")
      }
      val copied = buffer.copyToBuffer(b)
      buffer = buffer.drop(copied)
      b.flip()
    }

    /**
     * When potentially complete packet data are left after unwrap() we must
     * put them back onto the chopping block because otherwise the pump will
     * not know that we are runnable.
     */
    def putBack(b: ByteBuffer): Unit =
      if (b.hasRemaining) {
        if (tracing) log.debug(s"putting back ${b.remaining} bytes into $name")
        val bs = ByteString(b)
        if (bs.nonEmpty) buffer = bs ++ buffer
        prepare(b)
      }

    /**
     * Prepare a fresh ByteBuffer for receiving a chop of data.
     */
    def prepare(b: ByteBuffer): Unit = {
      b.clear()
      b.limit(0)
    }
  }

  // These are Netty's default values
  // 16665 + 1024 (room for compressed data) + 1024 (for OpenJDK compatibility)
  private val transportOutBuffer = ByteBuffer.allocate(16665 + 2048)
  /*
   * deviating here: chopping multiple input packets into this buffer can lead to
   * an OVERFLOW signal that also is an UNDERFLOW; avoid unnecessary copying by
   * increasing this buffer size to host up to two packets
   */
  private val userOutBuffer = ByteBuffer.allocate(16665 * 2 + 2048)
  private val transportInBuffer = ByteBuffer.allocate(16665 + 2048)
  private val userInBuffer = ByteBuffer.allocate(16665 + 2048)

  private val userInChoppingBlock = new ChoppingBlock(UserIn, "UserIn")
  userInChoppingBlock.prepare(userInBuffer)
  private val transportInChoppingBlock = new ChoppingBlock(TransportIn, "TransportIn")
  transportInChoppingBlock.prepare(transportInBuffer)

  var lastHandshakeStatus: HandshakeStatus = null
  var corkUser = true

  // The engine could also be instantiated in ActorMaterializerImpl but if creation fails
  // during materialization it would be worse than failing later on.
  val engine =
    try createSSLEngine()
    catch { case NonFatal(ex) => fail(ex, closeTransport = true); throw ex }

  engine.beginHandshake()
  lastHandshakeStatus = engine.getHandshakeStatus

  var currentSession = engine.getSession

  def setNewSessionParameters(params: NegotiateNewSession): Unit = {
    if (tracing) log.debug(s"applying $params")
    currentSession.invalidate()
    TlsUtils.applySessionParameters(engine, params)
    engine.beginHandshake()
    lastHandshakeStatus = engine.getHandshakeStatus
    corkUser = true
  }

  /*
   * So here’s the big picture summary: the SSLEngine is the boss, and it can
   * be in several states. Depending on this state, we may want to react to
   * different input and output conditions.
   *
   *  - normal bidirectional operation (does both outbound and inbound)
   *  - outbound close initiated, inbound still open
   *  - inbound close initiated, outbound still open
   *  - fully closed
   *
   * Upon reaching the last state we just shut down. In addition to
   * these user-data states, the engine may at any point in time also be
   * handshaking. This is mostly transparent, but it has an influence on the
   * outbound direction:
   *
   *  - if the local user triggered a re-negotiation, cork all user data until
   *    that is finished
   *  - if the outbound direction has been closed, trigger outbound readiness
   *    based upon HandshakeStatus.NEED_WRAP
   *
   * These conditions lead to the introduction of a synthetic TransferState
   * representing the Engine.
   */

  val engineNeedsWrap = new TransferState {
    def isReady = lastHandshakeStatus == NEED_WRAP
    def isCompleted = engine.isOutboundDone
  }

  val engineInboundOpen = new TransferState {
    def isReady = true
    def isCompleted = engine.isInboundDone
  }

  val userHasData = new TransferState {
    def isReady = !corkUser && userInChoppingBlock.isReady && lastHandshakeStatus != NEED_UNWRAP
    def isCompleted = inputBunch.isCancelled(UserIn) || inputBunch.isDepleted(UserIn)
  }

  val userOutCancelled = new TransferState {
    def isReady = outputBunch.isCancelled(UserOut)
    def isCompleted = engine.isInboundDone || outputBunch.isErrored(UserOut)
  }

  // bidirectional case
  val outbound = (userHasData || engineNeedsWrap) && outputBunch.demandAvailableFor(TransportOut)
  val inbound = (transportInChoppingBlock && outputBunch.demandAvailableFor(UserOut)) || userOutCancelled

  // half-closed
  val outboundHalfClosed = engineNeedsWrap && outputBunch.demandAvailableFor(TransportOut)
  val inboundHalfClosed = transportInChoppingBlock && engineInboundOpen

  val bidirectional = TransferPhase(outbound || inbound) { () =>
    if (tracing) log.debug("bidirectional")
    val continue = doInbound(isOutboundClosed = false, inbound)
    if (continue) {
      if (tracing) log.debug("bidirectional continue")
      doOutbound(isInboundClosed = false)
    }
  }

  val flushingOutbound = TransferPhase(outboundHalfClosed) { () =>
    if (tracing) log.debug("flushingOutbound")
    try doWrap()
    catch { case _: SSLException => nextPhase(completedPhase) }
  }

  val awaitingClose = TransferPhase(inputBunch.inputsAvailableFor(TransportIn) && engineInboundOpen) { () =>
    if (tracing) log.debug("awaitingClose")
    transportInChoppingBlock.chopInto(transportInBuffer)
    try doUnwrap(ignoreOutput = true)
    catch { case _: SSLException => nextPhase(completedPhase) }
  }

  val outboundClosed = TransferPhase(outboundHalfClosed || inbound) { () =>
    if (tracing) log.debug("outboundClosed")
    val continue = doInbound(isOutboundClosed = true, inbound)
    if (continue && outboundHalfClosed.isReady) {
      if (tracing) log.debug("outboundClosed continue")
      try doWrap()
      catch { case _: SSLException => nextPhase(completedPhase) }
    }
  }

  val inboundClosed = TransferPhase(outbound || inboundHalfClosed) { () =>
    if (tracing) log.debug("inboundClosed")
    val continue = doInbound(isOutboundClosed = false, inboundHalfClosed)
    if (continue) {
      if (tracing) log.debug("inboundClosed continue")
      doOutbound(isInboundClosed = true)
    }
  }

  def completeOrFlush(): Unit =
    if (engine.isOutboundDone || (engine.isInboundDone && userInChoppingBlock.isEmpty)) nextPhase(completedPhase)
    else nextPhase(flushingOutbound)

  private def doInbound(isOutboundClosed: Boolean, inboundState: TransferState): Boolean =
    if (inputBunch.isDepleted(TransportIn) && transportInChoppingBlock.isEmpty) {
      if (tracing) log.debug("closing inbound")
      try engine.closeInbound()
      catch { case _: SSLException => outputBunch.enqueue(UserOut, SessionTruncated) }
      lastHandshakeStatus = engine.getHandshakeStatus
      completeOrFlush()
      false
    } else if (inboundState != inboundHalfClosed && outputBunch.isCancelled(UserOut)) {
      if (!isOutboundClosed && closing.ignoreCancel) {
        if (tracing) log.debug("ignoring UserIn cancellation")
        nextPhase(inboundClosed)
      } else {
        if (tracing) log.debug("closing inbound due to UserOut cancellation")
        engine.closeOutbound() // this is the correct way of shutting down the engine
        lastHandshakeStatus = engine.getHandshakeStatus
        nextPhase(flushingOutbound)
      }
      true
    } else if (inboundState.isReady) {
      transportInChoppingBlock.chopInto(transportInBuffer)
      try {
        doUnwrap(ignoreOutput = false)
        true
      } catch {
        case ex: SSLException =>
          if (tracing) log.debug(s"SSLException during doUnwrap: $ex")
          fail(ex, closeTransport = false)
          engine.closeInbound() // we don't need to add lastHandshakeStatus check here because
          completeOrFlush() // it doesn't make any sense to write anything to the network anymore
          false
      }
    } else true

  private def doOutbound(isInboundClosed: Boolean): Unit =
    if (inputBunch.isDepleted(UserIn) && userInChoppingBlock.isEmpty && mayCloseOutbound) {
      if (!isInboundClosed && closing.ignoreComplete) {
        if (tracing) log.debug("ignoring closeOutbound")
      } else {
        if (tracing) log.debug("closing outbound directly")
        engine.closeOutbound()
        lastHandshakeStatus = engine.getHandshakeStatus
      }
      nextPhase(outboundClosed)
    } else if (outputBunch.isCancelled(TransportOut)) {
      if (tracing) log.debug("shutting down because TransportOut is cancelled")
      nextPhase(completedPhase)
    } else if (outbound.isReady) {
      if (userHasData.isReady) userInChoppingBlock.chopInto(userInBuffer)
      try doWrap()
      catch {
        case ex: SSLException =>
          if (tracing) log.debug(s"SSLException during doWrap: $ex")
          fail(ex, closeTransport = false)
          completeOrFlush()
      }
    }

  /**
   * In JDK 8 it is not allowed to call `closeOutbound` before the handshake is done or otherwise
   * an IllegalStateException might be thrown when the next handshake packet arrives.
   */
  private def mayCloseOutbound: Boolean =
    lastHandshakeStatus match {
      case HandshakeStatus.NOT_HANDSHAKING | HandshakeStatus.FINISHED => true
      case _                                                          => false
    }

  def flushToTransport(): Unit = {
    if (tracing) log.debug("flushToTransport")
    transportOutBuffer.flip()
    if (transportOutBuffer.hasRemaining) {
      val bs = ByteString(transportOutBuffer)
      outputBunch.enqueue(TransportOut, bs)
      if (tracing) log.debug(s"sending ${bs.size} bytes")
    }
    transportOutBuffer.clear()
  }

  def flushToUser(): Unit = {
    if (tracing) log.debug("flushToUser")
    if (unwrapPutBackCounter > 0) unwrapPutBackCounter = 0
    userOutBuffer.flip()
    if (userOutBuffer.hasRemaining) {
      val bs = ByteString(userOutBuffer)
      outputBunch.enqueue(UserOut, SessionBytes(currentSession, bs))
    }
    userOutBuffer.clear()
  }

  private def doWrap(): Unit = {
    val result = engine.wrap(userInBuffer, transportOutBuffer)
    lastHandshakeStatus = result.getHandshakeStatus
    if (tracing)
      log.debug(
        s"wrap: status=${result.getStatus} handshake=$lastHandshakeStatus remaining=${userInBuffer.remaining} out=${transportOutBuffer
          .position()}")

    if (lastHandshakeStatus == FINISHED) handshakeFinished()
    runDelegatedTasks()
    result.getStatus match {
      case OK =>
        // https://github.com/akka/akka-core/issues/29922
        // It seems to be possible to get the SSLEngine into a state where
        // result.getStatus == OK && getHandshakeStatus == NEED_WRAP but
        // it doesn't make any progress any more.
        //
        // We guard against this JDK bug by checking for reasonable invariants after the call to engine.wrap
        if (transportOutBuffer.position() == 0 && lastHandshakeStatus == NEED_WRAP)
          throw new IllegalStateException("SSLEngine trying to loop NEED_WRAP without producing output")

        flushToTransport()
        userInChoppingBlock.putBack(userInBuffer)
      case CLOSED =>
        flushToTransport()
        if (engine.isInboundDone) nextPhase(completedPhase)
        else nextPhase(awaitingClose)
      case s => fail(new IllegalStateException(s"unexpected status $s in doWrap()"))
    }
  }

  @tailrec
  private def doUnwrap(ignoreOutput: Boolean): Unit = {
    val oldInPosition = transportInBuffer.position()
    val result = engine.unwrap(transportInBuffer, userOutBuffer)
    if (ignoreOutput) userOutBuffer.clear()
    lastHandshakeStatus = result.getHandshakeStatus
    if (tracing)
      log.debug(
        s"unwrap: status=${result.getStatus} handshake=$lastHandshakeStatus remaining=${transportInBuffer.remaining} out=${userOutBuffer
          .position()}")
    runDelegatedTasks()
    result.getStatus match {
      case OK =>
        result.getHandshakeStatus match {
          case NEED_WRAP =>
            // https://github.com/akka/akka-core/issues/29922
            // A second workaround for an infinite loop we have not been able to reproduce/isolate,
            // if you see this, and can reproduce consistently, please report back to the Akka team
            // with a reproducer or details about the client causing it
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
      case null => fail(new IllegalStateException(s"unexpected status 'null' in doUnwrap()"))
    }
  }

  @tailrec
  private def runDelegatedTasks(): Unit = {
    val task = engine.getDelegatedTask
    if (task != null) {
      if (tracing) log.debug("running task")
      task.run()
      runDelegatedTasks()
    } else {
      val st = lastHandshakeStatus
      lastHandshakeStatus = engine.getHandshakeStatus
      if (tracing && st != lastHandshakeStatus) log.debug(s"handshake status after tasks: $lastHandshakeStatus")
    }
  }

  private def handshakeFinished(): Unit = {
    if (tracing) log.debug("handshake finished")
    val session = engine.getSession

    verifySession(session) match {
      case Success(()) =>
        currentSession = session
        corkUser = false
        flushToUser()
      case Failure(ex) =>
        fail(ex, closeTransport = true)
    }
  }

  override def receive = inputBunch.subreceive.orElse[Any, Unit](outputBunch.subreceive).orElse {
    case ActorGraphInterpreter.Snapshot =>
      sender() ! StreamSnapshotImpl(self.path, Seq.empty, Seq.empty)
  }

  initialPhase(2, bidirectional)

  protected def fail(e: Throwable, closeTransport: Boolean = true): Unit = {
    if (tracing) log.debug("fail {} due to: {}", self, e.getMessage)
    inputBunch.cancel()
    if (closeTransport) {
      log.debug("closing output")
      outputBunch.error(TransportOut, e)
    }
    outputBunch.error(UserOut, e)
    stopped = true
    pump()
  }

  override def postStop(): Unit = {
    if (!stopped) {
      val e = new RuntimeException(
        s"Unexpected termination of TLS actor for connection to ${engine.getPeerHost}:${engine.getPeerPort}")
      log.warning(e.getMessage)
      inputBunch.cancel()
      outputBunch.error(TransportOut, e)
      outputBunch.error(UserOut, e)
    } else if (tracing) {
      log.debug("postStop")
    }
    super.postStop()
  }

  override protected def pumpFailed(e: Throwable): Unit = fail(e)

  override protected def pumpFinished(): Unit = {
    inputBunch.cancel()
    outputBunch.complete()
    if (tracing) log.debug(s"STOP Outbound Closed: ${engine.isOutboundDone} Inbound closed: ${engine.isInboundDone}")
    stopped = true
    context.stop(self)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TlsUtils {
  def applySessionParameters(engine: SSLEngine, sessionParameters: NegotiateNewSession): Unit = {
    sessionParameters.enabledCipherSuites.foreach(cs => engine.setEnabledCipherSuites(cs.toArray))
    sessionParameters.enabledProtocols.foreach(p => engine.setEnabledProtocols(p.toArray))

    sessionParameters.sslParameters.foreach(engine.setSSLParameters)

    sessionParameters.clientAuth match {
      case Some(TLSClientAuth.None) => engine.setNeedClientAuth(false)
      case Some(TLSClientAuth.Want) => engine.setWantClientAuth(true)
      case Some(TLSClientAuth.Need) => engine.setNeedClientAuth(true)
      case _                        => // do nothing
    }
  }

  def cloneParameters(old: SSLParameters): SSLParameters = {
    val newParameters = new SSLParameters()
    newParameters.setAlgorithmConstraints(old.getAlgorithmConstraints)
    newParameters.setCipherSuites(old.getCipherSuites)
    newParameters.setEndpointIdentificationAlgorithm(old.getEndpointIdentificationAlgorithm)
    newParameters.setNeedClientAuth(old.getNeedClientAuth)
    newParameters.setProtocols(old.getProtocols)
    newParameters.setServerNames(old.getServerNames)
    newParameters.setSNIMatchers(old.getSNIMatchers)
    newParameters.setUseCipherSuitesOrder(old.getUseCipherSuitesOrder)
    newParameters.setWantClientAuth(old.getWantClientAuth)
    newParameters
  }
}
