/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.io.IOException
import java.net.{ InetSocketAddress, SocketException }
import java.nio.ByteBuffer
import java.nio.channels.{ FileChannel, SocketChannel }
import java.nio.channels.SelectionKey._
import java.nio.file.{ Path, Paths }

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.control.{ NoStackTrace, NonFatal }

import akka.actor._
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.io.Inet.SocketOption
import akka.io.SelectionHandler._
import akka.io.Tcp._
import akka.util.ByteString

/**
 * Base class for TcpIncomingConnection and TcpOutgoingConnection.
 *
 * INTERNAL API
 */
private[io] abstract class TcpConnection(val tcp: TcpExt, val channel: SocketChannel, val pullMode: Boolean)
    extends Actor
    with ActorLogging
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

  import TcpConnection._
  import tcp.Settings._
  import tcp.bufferPool

  private[this] var pendingWrite: PendingWrite = EmptyPendingWrite
  private[this] var peerClosed = false
  private[this] var writingSuspended = false
  private[this] var readingSuspended = pullMode
  private[this] var interestedInResume: Option[ActorRef] = None
  private[this] var closedMessage: Option[CloseInformation] = None // for ConnectionClosed message in postStop
  private var watchedActor: ActorRef = context.system.deadLetters
  private var registration: Option[ChannelRegistration] = None

  def setRegistration(registration: ChannelRegistration): Unit = this.registration = Some(registration)
  protected def signDeathPact(actor: ActorRef): Unit = {
    unsignDeathPact()
    watchedActor = actor
    context.watch(watchedActor)
  }

  protected def unsignDeathPact(): Unit =
    if (watchedActor ne context.system.deadLetters) context.unwatch(watchedActor)

  def writePending = pendingWrite ne EmptyPendingWrite

  // STATES

  /** connection established, waiting for registration from user handler */
  def waitingForRegistration(registration: ChannelRegistration, commander: ActorRef): Receive = {
    case Register(handler, keepOpenOnPeerClosed, useResumeWriting) =>
      // up to this point we've been watching the commander,
      // but since registration is now complete we only need to watch the handler from here on
      if (handler != commander)
        signDeathPact(handler) // will unsign death pact with commander automatically

      if (TraceLogging) log.debug("[{}] registered as connection handler", handler)

      val info = ConnectionInfo(registration, handler, keepOpenOnPeerClosed, useResumeWriting)

      context.setReceiveTimeout(Duration.Undefined)
      context.become(connected(info))

      // if we are in push mode or already have resumed reading in pullMode while waiting for Register
      // then register OP_READ interest
      if (!pullMode || (/*pullMode && */ !readingSuspended)) resumeReading(info, None)

    case ResumeReading =>
      readingSuspended = false

    case SuspendReading =>
      readingSuspended = true

    case cmd: CloseCommand =>
      val info = ConnectionInfo(registration, commander, keepOpenOnPeerClosed = false, useResumeWriting = false)
      handleClose(info, Some(sender()), cmd.event)

    case ReceiveTimeout =>
      // after sending `Register` user should watch this actor to make sure
      // it didn't die because of the timeout
      log.debug("Configured registration timeout of [{}] expired, stopping", RegisterTimeout)
      context.stop(self)
  }

  /** normal connected state */
  def connected(info: ConnectionInfo): Receive =
    handleWriteMessages(info).orElse {
      case SuspendReading    => suspendReading(info)
      case ResumeReading     => resumeReading(info, None)
      case ChannelReadable   => doRead(info, None)
      case cmd: CloseCommand => handleClose(info, Some(sender()), cmd.event)
    }

  /** the peer sent EOF first, but we may still want to send */
  def peerSentEOF(info: ConnectionInfo): Receive =
    handleWriteMessages(info).orElse {
      case cmd: CloseCommand               => handleClose(info, Some(sender()), cmd.event)
      case ResumeReading | ChannelReadable => // ignore, no more data to read
    }

  /** connection is closing but a write has to be finished first */
  def closingWithPendingWrite(
      info: ConnectionInfo,
      closeCommander: Option[ActorRef],
      closedEvent: ConnectionClosed): Receive = {
    case SuspendReading  => suspendReading(info)
    case ResumeReading   => resumeReading(info, closeCommander)
    case ChannelReadable => doRead(info, closeCommander)

    case ChannelWritable =>
      doWrite(info)
      if (!writePending) // writing is now finished
        handleClose(info, closeCommander, closedEvent)

    case UpdatePendingWriteAndThen(remaining, work) =>
      pendingWrite = remaining
      work()
      if (writePending) info.registration.enableInterest(OP_WRITE)
      else handleClose(info, closeCommander, closedEvent)

    case WriteFileFailed(e) => handleError(info.handler, e) // rethrow exception from dispatcher task

    case Abort => handleClose(info, Some(sender()), Aborted)
  }

  /** connection is closed on our side and we're waiting from confirmation from the other side */
  def closing(info: ConnectionInfo, closeCommander: Option[ActorRef]): Receive = {
    case SuspendReading  => suspendReading(info)
    case ResumeReading   => resumeReading(info, closeCommander)
    case ChannelReadable => doRead(info, closeCommander)
    case Close           => doCloseConnection(info.handler, closeCommander, Close.event)
    case Abort           => handleClose(info, Some(sender()), Aborted)
  }

  def handleWriteMessages(info: ConnectionInfo): Receive = {
    case ChannelWritable =>
      if (writePending) {
        doWrite(info)
        if (!writePending && interestedInResume.nonEmpty) {
          interestedInResume.get ! WritingResumed
          interestedInResume = None
        }
      }

    case write: WriteCommand =>
      if (writingSuspended) {
        if (TraceLogging) log.debug("Dropping write because writing is suspended")
        sender() ! CommandFailed(write).withCause(DroppingWriteBecauseWritingIsSuspendedException)

      } else if (writePending) {
        if (TraceLogging) log.debug("Dropping write because queue is full")
        sender() ! CommandFailed(write).withCause(DroppingWriteBecauseQueueIsFullException)
        if (info.useResumeWriting) writingSuspended = true

      } else {
        pendingWrite = PendingWrite(sender(), write)
        if (writePending) doWrite(info)
      }

    case ResumeWriting =>
      /*
       * If more than one actor sends Writes then the first to send this
       * message might resume too early for the second, leading to a Write of
       * the second to go through although it has not been resumed yet; there
       * is nothing we can do about this apart from all actors needing to
       * register themselves and us keeping track of them, which sounds bad.
       *
       * Thus it is documented that useResumeWriting is incompatible with
       * multiple writers. But we fail as gracefully as we can.
       */
      writingSuspended = false
      if (writePending) {
        if (interestedInResume.isEmpty) interestedInResume = Some(sender())
        else sender() ! CommandFailed(ResumeWriting)
      } else sender() ! WritingResumed

    case UpdatePendingWriteAndThen(remaining, work) =>
      pendingWrite = remaining
      work()
      if (writePending) info.registration.enableInterest(OP_WRITE)

    case WriteFileFailed(e) => handleError(info.handler, e) // rethrow exception from dispatcher task
  }

  /** stopWith sets this state while waiting for the SelectionHandler to execute the `cancelAndClose` thunk */
  def unregistering: Receive = {
    case Unregistered => context.stop(self) // postStop will notify interested parties
    case _            =>
    // Ignore everything else, we might end up here without user interaction, e.g. if the peer sends a RST packet
    // In this case, we notify the user handler, which might have already concurrently sent us more commands
    // that we can only drop at this point.
  }

  // AUXILIARIES and IMPLEMENTATION

  /** used in subclasses to start the common machinery above once a channel is connected */
  @nowarn("msg=deprecated")
  def completeConnect(
      registration: ChannelRegistration,
      commander: ActorRef,
      options: immutable.Traversable[SocketOption]): Unit = {
    this.registration = Some(registration)

    // Turn off Nagle's algorithm by default
    try channel.socket.setTcpNoDelay(true)
    catch {
      case e: SocketException =>
        // as reported in #16653 some versions of netcat (`nc -z`) doesn't allow setTcpNoDelay
        // continue anyway
        log.debug("Could not enable TcpNoDelay: {}", e.getMessage)
    }
    options.foreach(_.afterConnect(channel.socket))

    commander ! Connected(
      channel.socket.getRemoteSocketAddress.asInstanceOf[InetSocketAddress],
      channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress])

    context.setReceiveTimeout(RegisterTimeout)

    // !!WARNING!! The line below is needed to make Windows notify us about aborted connections, see #15766
    if (WindowsConnectionAbortWorkaroundEnabled) registration.enableInterest(OP_CONNECT)

    context.become(waitingForRegistration(registration, commander))
  }

  def suspendReading(info: ConnectionInfo): Unit = {
    readingSuspended = true
    info.registration.disableInterest(OP_READ)
  }
  def resumeReading(info: ConnectionInfo, closeCommander: Option[ActorRef]): Unit = {
    readingSuspended = false
    doRead(info, closeCommander)
  }

  /**
   * Read from the channel and potentially send out `Received` message to handler.
   *
   * In some cases, this method will change the state with `context.become`.
   */
  def doRead(info: ConnectionInfo, closeCommander: Option[ActorRef]): Unit =
    if (!readingSuspended) {
      @tailrec def innerRead(buffer: ByteBuffer, remainingLimit: Int): ReadResult =
        if (remainingLimit > 0) {
          // never read more than the configured limit
          buffer.clear()
          val maxBufferSpace = math.min(DirectBufferSize, remainingLimit)
          buffer.limit(maxBufferSpace)
          val readBytes = channel.read(buffer)
          buffer.flip()

          if (TraceLogging) log.debug("Read [{}] bytes.", readBytes)
          if (readBytes > 0) info.handler ! Received(ByteString(buffer))

          readBytes match {
            case `maxBufferSpace` =>
              if (pullMode) MoreDataWaiting else innerRead(buffer, remainingLimit - maxBufferSpace)
            case x if x >= 0 =>
              if (!pullMode || x == 0)
                // if (pullMode) we reach here by probing from resumeReading,
                // otherwise we have just exhausted the network receive buffer.
                // In any case, we now want to be notified about more data being available
                info.registration.enableInterest(OP_READ)

              AllRead
            case -1 => EndOfStream
            case _ =>
              throw new IllegalStateException("Unexpected value returned from read: " + readBytes)
          }
        } else MoreDataWaiting

      val buffer = bufferPool.acquire()
      try innerRead(buffer, ReceivedMessageSizeLimit) match {
        case AllRead => // nothing to do
        case MoreDataWaiting =>
          if (!pullMode) self ! ChannelReadable
        case EndOfStream if channel.socket.isOutputShutdown =>
          if (TraceLogging) log.debug("Read returned end-of-stream, our side already closed")
          doCloseConnection(info.handler, closeCommander, ConfirmedClosed)
        case EndOfStream =>
          if (TraceLogging) log.debug("Read returned end-of-stream, our side not yet closed")
          handleClose(info, closeCommander, PeerClosed)
      } catch {
        case e: IOException => handleError(info.handler, e)
      } finally bufferPool.release(buffer)
    }

  def doWrite(info: ConnectionInfo): Unit = pendingWrite = pendingWrite.doWrite(info)

  def closeReason =
    if (channel.socket.isOutputShutdown) ConfirmedClosed
    else PeerClosed

  def handleClose(info: ConnectionInfo, closeCommander: Option[ActorRef], closedEvent: ConnectionClosed): Unit =
    closedEvent match {
      case Aborted =>
        if (TraceLogging) log.debug("Got Abort command. RESETing connection.")
        doCloseConnection(info.handler, closeCommander, closedEvent)
      case PeerClosed if info.keepOpenOnPeerClosed =>
        // report that peer closed the connection
        info.handler ! PeerClosed
        // used to check if peer already closed its side later
        peerClosed = true
        context.become(peerSentEOF(info))
      case _ if writePending => // finish writing first
        // Our registered actor is now free to terminate cleanly
        unsignDeathPact()
        if (TraceLogging) log.debug("Got Close command but write is still pending.")
        context.become(closingWithPendingWrite(info, closeCommander, closedEvent))
      case ConfirmedClosed => // shutdown output and wait for confirmation
        if (TraceLogging) log.debug("Got ConfirmedClose command, sending FIN.")

        // If peer closed first, the socket is now fully closed.
        // Also, if shutdownOutput threw an exception we expect this to be an indication
        // that the peer closed first or concurrently with this code running.
        // also see https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4516760
        if (peerClosed || !safeShutdownOutput())
          doCloseConnection(info.handler, closeCommander, closedEvent)
        else context.become(closing(info, closeCommander))
      case _ => // close now
        if (TraceLogging) log.debug("Got Close command, closing connection.")
        doCloseConnection(info.handler, closeCommander, closedEvent)
    }

  def doCloseConnection(handler: ActorRef, closeCommander: Option[ActorRef], closedEvent: ConnectionClosed): Unit = {
    stopWith(CloseInformation(Set(handler) ++ closeCommander, closedEvent))
  }

  def handleError(handler: ActorRef, exception: IOException): Unit = {
    log.debug("Closing connection due to IO error {}", exception)
    stopWith(CloseInformation(Set(handler), ErrorClosed(extractMsg(exception))))
  }

  def safeShutdownOutput(): Boolean =
    try {
      channel.socket().shutdownOutput()
      true
    } catch {
      case _: SocketException => false
    }

  @tailrec private[this] def extractMsg(t: Throwable): String =
    if (t == null) "unknown"
    else {
      t.getMessage match {
        case null | "" => extractMsg(t.getCause)
        case msg       => msg
      }
    }

  def prepareAbort(): Unit = {
    try channel.socket.setSoLinger(true, 0) // causes the following close() to send TCP RST
    catch {
      case NonFatal(e) =>
        // setSoLinger can fail due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6799574
        // (also affected: OS/X Java 1.6.0_37)
        if (TraceLogging) log.debug("setSoLinger(true, 0) failed with [{}]", e)
    }
    // Actual channel closing is done in stopWith or postStop by calling registration.cancelAndClose()
    // which makes sure the channel is flushed from the selector as well.

    // This is necessary because on Windows (and all platforms starting with JDK 11) the connection is merely added
    // to the `cancelledKeys` of the `java.nio.channels.spi.AbstractSelector`,
    // and `sun.nio.ch.SelectorImpl` will kill those from `processDeregisterQueue` after the select poll has returned.
  }

  def stopWith(closeInfo: CloseInformation, shouldAbort: Boolean = false): Unit = {
    closedMessage = Some(closeInfo)
    unsignDeathPact()

    if (closeInfo.closedEvent == Aborted || shouldAbort)
      prepareAbort()

    registration match {
      case None =>
        context.stop(self)
      case Some(reg) =>
        context.become(unregistering)
        reg.cancelAndClose(() => self ! Unregistered)
    }
  }

  override def postStop(): Unit = {
    if (writePending) pendingWrite.release()

    val interestedInClose: Set[ActorRef] =
      (if (writePending) Set(pendingWrite.commander) else Set.empty) ++
      closedMessage.toList.flatMap(_.notificationsTo).toSet

    if (channel.isOpen) // if channel is still open here, we didn't go through stopWith => unexpected actor termination
      prepareAbort()

    def isCommandFailed: Boolean = closedMessage.exists(_.closedEvent.isInstanceOf[CommandFailed])
    def notifyInterested(): Unit =
      for {
        msg <- closedMessage
        ref <- interestedInClose
      } ref ! msg.closedEvent

    if (!channel.isOpen || isCommandFailed || registration.isEmpty)
      // if channel was already closed we can send out notification directly
      notifyInterested()
    else
      // otherwise, we unregister and notify afterwards
      registration.foreach(_.cancelAndClose(() => notifyInterested()))
  }

  override def postRestart(reason: Throwable): Unit =
    throw new IllegalStateException("Restarting not supported for connection actors.")

  @nowarn("cat=deprecation")
  def PendingWrite(commander: ActorRef, write: WriteCommand): PendingWrite = {
    @tailrec def create(head: WriteCommand, tail: WriteCommand): PendingWrite =
      head match {
        case Write.empty                       => if (tail eq Write.empty) EmptyPendingWrite else create(tail, Write.empty)
        case Write(data, ack) if data.nonEmpty => PendingBufferWrite(commander, data, ack, tail)
        case WriteFile(path, offset, count, ack) =>
          PendingWriteFile(commander, Paths.get(path), offset, count, ack, tail)
        case WritePath(path, offset, count, ack) =>
          PendingWriteFile(commander, path, offset, count, ack, tail)
        case CompoundWrite(h, t) => create(h, t)
        case x @ Write(_, ack) => // empty write with either an ACK or a non-standard NoACK
          if (x.wantsAck) commander ! ack
          create(tail, Write.empty)
      }
    create(write, Write.empty)
  }

  def PendingBufferWrite(commander: ActorRef, data: ByteString, ack: Event, tail: WriteCommand): PendingBufferWrite = {
    val buffer = bufferPool.acquire()
    try {
      val copied = data.copyToBuffer(buffer)
      buffer.flip()
      new PendingBufferWrite(commander, data.drop(copied), ack, buffer, tail)
    } catch {
      case NonFatal(e) =>
        bufferPool.release(buffer)
        throw e
    }
  }

  class PendingBufferWrite(
      val commander: ActorRef,
      remainingData: ByteString,
      ack: Any,
      buffer: ByteBuffer,
      tail: WriteCommand)
      extends PendingWrite {

    def doWrite(info: ConnectionInfo): PendingWrite = {
      @tailrec def writeToChannel(data: ByteString): PendingWrite = {
        val writtenBytes = channel.write(buffer) // at first we try to drain the remaining bytes from the buffer
        if (TraceLogging) log.debug("Wrote [{}] bytes to channel", writtenBytes)
        if (buffer.hasRemaining) {
          // we weren't able to write all bytes from the buffer, so we need to try again later
          if (data eq remainingData) this
          else new PendingBufferWrite(commander, data, ack, buffer, tail) // copy with updated remainingData

        } else if (data.nonEmpty) {
          buffer.clear()
          val copied = data.copyToBuffer(buffer)
          buffer.flip()
          writeToChannel(data.drop(copied))

        } else {
          if (!ack.isInstanceOf[NoAck]) commander ! ack
          release()
          PendingWrite(commander, tail)
        }
      }
      try {
        val next = writeToChannel(remainingData)
        if (next ne EmptyPendingWrite) info.registration.enableInterest(OP_WRITE)
        next
      } catch { case e: IOException => handleError(info.handler, e); this }
    }

    def release(): Unit = bufferPool.release(buffer)
  }

  def PendingWriteFile(
      commander: ActorRef,
      filePath: Path,
      offset: Long,
      count: Long,
      ack: Event,
      tail: WriteCommand): PendingWriteFile =
    new PendingWriteFile(commander, FileChannel.open(filePath), offset, count, ack, tail)

  class PendingWriteFile(
      val commander: ActorRef,
      fileChannel: FileChannel,
      offset: Long,
      remaining: Long,
      ack: Event,
      tail: WriteCommand)
      extends PendingWrite
      with Runnable {

    def doWrite(info: ConnectionInfo): PendingWrite = {
      tcp.fileIoDispatcher.execute(this)
      this
    }

    def release(): Unit = fileChannel.close()

    def run(): Unit =
      try {
        val toWrite = math.min(remaining, tcp.Settings.TransferToLimit)
        val written = fileChannel.transferTo(offset, toWrite, channel)

        if (written < remaining) {
          val updated = new PendingWriteFile(commander, fileChannel, offset + written, remaining - written, ack, tail)
          self ! UpdatePendingWriteAndThen(updated, TcpConnection.doNothing)
        } else {
          release()
          val andThen = if (!ack.isInstanceOf[NoAck])() => commander ! ack else doNothing
          self ! UpdatePendingWriteAndThen(PendingWrite(commander, tail), andThen)
        }
      } catch {
        case e: IOException => self ! WriteFileFailed(e)
      }
  }
}

/**
 * INTERNAL API
 */
private[io] object TcpConnection {
  sealed trait ReadResult
  object EndOfStream extends ReadResult
  object AllRead extends ReadResult
  object MoreDataWaiting extends ReadResult

  /**
   * Used to transport information to the postStop method to notify
   * interested party about a connection close.
   */
  final case class CloseInformation(notificationsTo: Set[ActorRef], closedEvent: Event)

  /**
   * Groups required connection-related data that are only available once the connection has been fully established.
   */
  final case class ConnectionInfo(
      registration: ChannelRegistration,
      handler: ActorRef,
      keepOpenOnPeerClosed: Boolean,
      useResumeWriting: Boolean)

  // INTERNAL MESSAGES

  final case class UpdatePendingWriteAndThen(remainingWrite: PendingWrite, work: () => Unit)
      extends NoSerializationVerificationNeeded
  final case class WriteFileFailed(e: IOException)
  case object Unregistered

  sealed abstract class PendingWrite {
    def commander: ActorRef
    def doWrite(info: ConnectionInfo): PendingWrite
    def release(): Unit // free any occupied resources
  }

  object EmptyPendingWrite extends PendingWrite {
    def commander: ActorRef = throw new IllegalStateException
    def doWrite(info: ConnectionInfo): PendingWrite = throw new IllegalStateException
    def release(): Unit = throw new IllegalStateException
  }

  val doNothing: () => Unit = () => ()

  val DroppingWriteBecauseWritingIsSuspendedException =
    new IOException("Dropping write because writing is suspended") with NoStackTrace

  val DroppingWriteBecauseQueueIsFullException =
    new IOException("Dropping write because queue is full") with NoStackTrace
}
