/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong }

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.{ Done, NotUsed }
import akka.actor.{ ActorRef, Terminated }
import akka.annotation.InternalApi
import akka.io.Inet.SocketOption
import akka.io.Tcp
import akka.io.Tcp._
import akka.stream._
import akka.stream.impl.ReactiveStreamsCompliance
import akka.stream.impl.fusing.GraphStages.detacher
import akka.stream.scaladsl.{ BidiFlow, Flow, TcpIdleTimeoutException, Tcp => StreamTcp }
import akka.stream.scaladsl.Tcp.{ OutgoingConnection, ServerBinding }
import akka.stream.scaladsl.TcpAttributes
import akka.stream.stage._
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[stream] class ConnectionSourceStage(
    val tcpManager: ActorRef,
    val endpoint: InetSocketAddress,
    val backlog: Int,
    val options: immutable.Iterable[SocketOption],
    val halfClose: Boolean,
    val idleTimeout: Duration,
    val bindShutdownTimeout: FiniteDuration)
    extends GraphStageWithMaterializedValue[SourceShape[StreamTcp.IncomingConnection], Future[StreamTcp.ServerBinding]] {
  import ConnectionSourceStage._

  val out: Outlet[StreamTcp.IncomingConnection] = Outlet("IncomingConnections.out")
  override def initialAttributes = Attributes.name("ConnectionSource")
  val shape: SourceShape[StreamTcp.IncomingConnection] = SourceShape(out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, Future[ServerBinding]) =
    throw new UnsupportedOperationException("Not used")

  // TODO: Timeout on bind
  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes,
      eagerMaterialzer: Materializer): (GraphStageLogic, Future[StreamTcp.ServerBinding]) = {
    val bindingPromise = Promise[ServerBinding]()

    val logic = new TimerGraphStageLogic(shape) with StageLogging {
      implicit def self: ActorRef = stageActor.ref

      val connectionFlowsAwaitingInitialization = new AtomicLong()
      var listener: ActorRef = _
      val unbindPromise = Promise[Unit]()
      var unbindStarted = false

      override def preStart(): Unit = {
        getStageActor(receive)
        tcpManager ! Tcp.Bind(self, endpoint, backlog, options, pullMode = true)
      }

      private def receive(evt: (ActorRef, Any)): Unit = {
        val sender = evt._1
        val msg = evt._2
        msg match {
          case Bound(localAddress) =>
            listener = sender
            stageActor.watch(listener)
            if (isAvailable(out)) listener ! ResumeAccepting(1)
            val thisStage = self
            bindingPromise.success(ServerBinding(localAddress)(() => {
              // To allow unbind() to be invoked multiple times with minimal chance of dead letters, we check if
              // it's already unbound before sending the message.
              if (!unbindPromise.isCompleted) {
                // Beware, sender must be explicit since stageActor.ref will be invalid to access after the stage
                // stopped.
                thisStage.tell(Unbind, thisStage)
              }
              unbindPromise.future
            }, unbindPromise.future.map(_ => Done)(ExecutionContext.parasitic)))
          case f: CommandFailed =>
            val ex = new BindFailedException {
              // cannot modify the actual exception class for compatibility reasons
              override def getMessage: String = s"Bind failed${f.causedByString}"
            }
            f.cause.foreach(ex.initCause)
            bindingPromise.failure(ex)
            unbindPromise.tryFailure(ex)
            failStage(ex)
          case c: Connected =>
            push(out, connectionFor(c, sender))
          case Unbind =>
            if (!isClosed(out) && (listener ne null)) tryUnbind()
          case Unbound =>
            unbindCompleted()
          case Terminated(ref) if ref == listener =>
            if (unbindStarted) {
              unbindCompleted()
            } else {
              val ex = new IllegalStateException(
                "IO Listener actor terminated unexpectedly for remote endpoint [" +
                endpoint.getHostString + ":" + endpoint.getPort + "]")
              unbindPromise.tryFailure(ex)
              failStage(ex)
            }
          case other =>
            log.warning("Unexpected message to TcpStage: [{}]", other.getClass)
        }
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            // Ignore if still binding
            if (listener ne null) listener ! ResumeAccepting(1)
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            if (log.isDebugEnabled) {
              cause match {
                case _: SubscriptionWithCancelException.NonFailureCancellation =>
                  log.debug(
                    "Unbinding from {}:{} because downstream cancelled stream",
                    endpoint.getHostString,
                    endpoint.getPort)
                case ex =>
                  log.debug(
                    "Unbinding from {}:{} because of downstream failure: {}",
                    endpoint.getHostString,
                    endpoint.getPort,
                    ex)
              }
            }
            tryUnbind()
          }
        })

      private def connectionFor(connected: Connected, connection: ActorRef): StreamTcp.IncomingConnection = {
        connectionFlowsAwaitingInitialization.incrementAndGet()

        val tcpFlow =
          Flow
            .fromGraph(
              new IncomingConnectionStage(
                connection,
                connected.remoteAddress,
                halfClose,
                () => connectionFlowsAwaitingInitialization.decrementAndGet()))
            .via(detacher[ByteString]) // must read ahead for proper completions

        // FIXME: Previous code was wrong, must add new tests
        val handler = idleTimeout match {
          case d: FiniteDuration => tcpFlow.join(TcpIdleTimeout(d, Some(connected.remoteAddress)))
          case _                 => tcpFlow
        }

        StreamTcp.IncomingConnection(connected.localAddress, connected.remoteAddress, handler)
      }

      private def tryUnbind(): Unit = {
        if ((listener ne null) && !unbindStarted) {
          unbindStarted = true
          setKeepGoing(true)
          listener ! Unbind
        }
      }

      private def unbindCompleted(): Unit = {
        stageActor.unwatch(listener)
        unbindPromise.trySuccess(())
        if (connectionFlowsAwaitingInitialization.get() == 0) completeStage()
        else scheduleOnce(BindShutdownTimer, bindShutdownTimeout)
      }

      override def onTimer(timerKey: Any): Unit = timerKey match {
        case BindShutdownTimer =>
          completeStage() // TODO need to manually shut down instead right?
        case other =>
          throw new IllegalArgumentException(s"Unknown timer key $other")
      }

      override def postStop(): Unit = {
        // a bit unexpected to succeed here rather than fail with abrupt stage termination
        // but there was an existing test case covering this behavior
        unbindPromise.trySuccess(())
        bindingPromise.tryFailure(new NoSuchElementException("Binding was unbound before it was completely finished"))
      }
    }

    (logic, bindingPromise.future)
  }

}

private[stream] object ConnectionSourceStage {
  val BindTimer = "BindTimer"
  val BindShutdownTimer = "BindTimer"
}

/**
 * INTERNAL API
 */
@InternalApi private[stream] object TcpConnectionStage {
  case object WriteAck extends Tcp.Event

  private case object WriteDelayAck extends Tcp.Event
  private val WriteDelayMessage = Write(ByteString.empty, WriteDelayAck)

  trait TcpRole {
    def halfClose: Boolean
  }
  case class Outbound(
      manager: ActorRef,
      connectCmd: Connect,
      localAddressPromise: Promise[InetSocketAddress],
      halfClose: Boolean)
      extends TcpRole

  case class Inbound(connection: ActorRef, halfClose: Boolean, registerCallback: () => Unit) extends TcpRole

  /*
   * This is a *non-detached* design, i.e. this does not prefetch itself any of the inputs. It relies on downstream
   * stages to provide the necessary prefetch on `bytesOut` and the framework to do the proper prefetch in the buffer
   * backing `bytesIn`. If prefetch on `bytesOut` is required (i.e. user stages cannot be trusted) then it is better
   * to attach an extra, fused buffer to the end of this flow. Keeping this stage non-detached makes it much simpler and
   * easier to maintain and understand.
   */
  class TcpStreamLogic(
      val shape: FlowShape[ByteString, ByteString],
      val role: TcpRole,
      inheritedAttributes: Attributes,
      remoteAddress: InetSocketAddress,
      eagerMaterializer: Materializer)
      extends GraphStageLogic(shape)
      with StageLogging {
    implicit def self: ActorRef = stageActor.ref

    private def bytesIn = shape.in
    private def bytesOut = shape.out

    // Set once (in preStart for inbound connections, in 'connecting' for outbound connections)
    // After that remains immutable
    private var connection: ActorRef = _

    @nowarn("msg=deprecated")
    private val writeBufferSize = inheritedAttributes
      .get[TcpAttributes.TcpWriteBufferSize](
        TcpAttributes.TcpWriteBufferSize(eagerMaterializer.settings.ioSettings.tcpWriteBufferSize))
      .size

    private var writeBuffer = ByteString.empty

    // there is data in-flight that we accepted from upstream but haven't successfully written to the connection yet
    private var writeInProgress = false
    // upstream already finished but are still writing the last data to the connection
    private var connectionClosePending = false

    @nowarn("msg=deprecated")
    private val coalesceWrites = eagerMaterializer.settings.ioSettings.coalesceWrites
    private def coalesceWritesDisabled = coalesceWrites == 0
    private var writeDelayCountDown = 0
    private var previousWriteBufferSize = 0

    // No reading until role have been decided
    setHandler(bytesOut, GraphStageLogic.EagerTerminateOutput)

    override def preStart(): Unit = {
      setKeepGoing(true)
      role match {
        case Inbound(conn, _, registerCallback) =>
          setHandler(bytesOut, readHandler)
          connection = conn
          getStageActor(connected).watch(connection)
          connection ! Register(self, keepOpenOnPeerClosed = true, useResumeWriting = false)
          registerCallback()
          pull(bytesIn)
        case ob @ Outbound(manager, cmd, _, _) =>
          getStageActor(connecting(ob)).watch(manager)
          manager ! cmd
        case other => throw new IllegalArgumentException(s"Unsupported TCP role: ${other}")
      }
    }

    // Only used for outbound connections
    private def connecting(ob: Outbound)(evt: (ActorRef, Any)): Unit = {
      val sender = evt._1
      val msg = evt._2
      msg match {
        case Terminated(_) => fail(new StreamTcpException("The IO manager actor (TCP) has terminated. Stopping now."))
        case f @ CommandFailed(cmd) =>
          fail(new StreamTcpException(s"Tcp command [$cmd] failed${f.causedByString}").initCause(f.cause.orNull))
        case c: Connected =>
          role.asInstanceOf[Outbound].localAddressPromise.success(c.localAddress)
          connection = sender
          setHandler(bytesOut, readHandler)
          stageActor.unwatch(ob.manager)
          stageActor.become(connected)
          stageActor.watch(connection)
          connection ! Register(self, keepOpenOnPeerClosed = true, useResumeWriting = false)
          if (isAvailable(bytesOut)) connection ! ResumeReading
          if (isClosed(bytesIn)) connection ! ConfirmedClose
          else pull(bytesIn)
        case other => log.warning("Unexpected message to connecting TcpStage: [{}]", other.getClass)
      }
    }

    private def sendWriteBuffer(): Unit = {
      connection ! Write(writeBuffer, WriteAck)
      writeInProgress = true
      writeBuffer = ByteString.empty
    }

    /*
     * Coalesce more frames by collecting more frames while waiting for round trip to the
     * connection actor. WriteDelayMessage is an empty Write message and WriteDelayAck will
     * be sent back as reply.
     */
    private def sendWriteDelay(): Unit = {
      previousWriteBufferSize = writeBuffer.length
      writeInProgress = true
      connection ! WriteDelayMessage
    }

    // Used for both inbound and outbound connections
    private def connected(evt: (ActorRef, Any)): Unit = {
      val msg = evt._2
      msg match {
        case Received(data) =>
          // Keep on reading even when closed. There is no "close-read-side" in TCP
          if (isClosed(bytesOut)) connection ! ResumeReading
          else push(bytesOut, data)

        case WriteDelayAck =>
          // Immediately flush the write buffer if no more frames have been collected during the WriteDelayMessage
          // round trip to the connection actor, or if reaching the configured maximum number of round trips, or
          // if writeBuffer capacity has been exceeded.
          writeDelayCountDown -= 1
          if (writeDelayCountDown == 0 || previousWriteBufferSize == writeBuffer.length || writeBuffer.length >= writeBufferSize)
            sendWriteBuffer()
          else
            sendWriteDelay()

        case WriteAck =>
          if (writeBuffer.isEmpty)
            writeInProgress = false
          else if (coalesceWritesDisabled || writeBuffer.length >= writeBufferSize)
            sendWriteBuffer()
          else {
            writeDelayCountDown = coalesceWrites
            sendWriteDelay()
          }

          if (!writeInProgress && connectionClosePending) {
            closeConnectionUpstreamFinished()
          }

          if (!isClosed(bytesIn) && !hasBeenPulled(bytesIn))
            pull(bytesIn)

        case Terminated(_) => fail(new StreamTcpException("The connection actor has terminated. Stopping now."))
        case f @ CommandFailed(cmd) =>
          fail(new StreamTcpException(s"Tcp command [$cmd] failed${f.causedByString}").initCause(f.cause.orNull))
        case ErrorClosed(cause) => fail(new StreamTcpException(s"The connection closed with error: $cause"))
        case Aborted            => fail(new StreamTcpException("The connection has been aborted"))
        case Closed             => completeStage()
        case ConfirmedClosed    => completeStage()
        case PeerClosed         => complete(bytesOut)
        case other =>
          log.warning("Unexpected message to connected TcpStage: [{}]", other.getClass)
      }
    }

    private def closeConnectionUpstreamFinished(): Unit = {
      if (isClosed(bytesOut) || !role.halfClose) {
        // Reading has stopped before, either because of cancel, or PeerClosed, so just Close now
        // (or half-close is turned off)
        if (writeInProgress)
          connectionClosePending = true // will continue when WriteAck is received and writeBuffer drained
        else
          connection ! Close
      } else if (connection != null) {
        // We still read, so we only close the write side
        if (writeInProgress)
          connectionClosePending = true // will continue when WriteAck is received and writeBuffer drained
        else
          connection ! ConfirmedClose
      }
      // Otherwise, this is an outbound connection with half-close enabled for which upstream finished
      // before the connection was even established.
      // In that case we half-close the connection as soon as it's connected
    }

    private def closeConnectionDownstreamFinished(): Unit = {
      if (connection == null) {
        // This is an outbound connection for which downstream finished
        // before the connection was even established.
        // In that case we close the connection as soon as upstream finishes
      } else {
        if (role.halfClose) {
          if (isClosed(bytesIn) && !writeInProgress)
            connection ! Close
          else
            connection ! ResumeReading
        } else if (!writeInProgress) {
          connection ! Close
        }
      }
    }

    val readHandler = new OutHandler {
      override def onPull(): Unit = {
        connection ! ResumeReading
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        cause match {
          case _: SubscriptionWithCancelException.NonFailureCancellation =>
            log.debug(
              "Closing connection from {}:{} because downstream cancelled stream without failure",
              remoteAddress.getHostString,
              remoteAddress.getPort)
            closeConnectionDownstreamFinished()
          case ex =>
            log.debug(
              "Aborting connection from {}:{} because of downstream failure: {}",
              remoteAddress.getHostString,
              remoteAddress.getPort,
              ex)
            connection ! Abort
            failStage(cause)
        }
      }
    }

    setHandler(
      bytesIn,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(bytesIn)
          ReactiveStreamsCompliance.requireNonNullElement(elem)
          if (writeInProgress) {
            writeBuffer = writeBuffer ++ elem
          } else if (coalesceWritesDisabled || writeBuffer.length >= writeBufferSize) {
            writeBuffer = writeBuffer ++ elem
            sendWriteBuffer()
          } else {
            writeBuffer = writeBuffer ++ elem
            writeDelayCountDown = coalesceWrites
            sendWriteDelay()
          }
          if (writeBuffer.length < writeBufferSize)
            pull(bytesIn)
        }

        override def onUpstreamFinish(): Unit =
          closeConnectionUpstreamFinished()

        override def onUpstreamFailure(ex: Throwable): Unit = {
          if (connection != null) {
            if (interpreter.log.isDebugEnabled) {
              val msg = "Aborting tcp connection to {} because of upstream failure: {}"

              if (ex.getStackTrace.isEmpty) interpreter.log.debug(msg, remoteAddress, ex)
              else interpreter.log.debug(msg + "\n{}", remoteAddress, ex, ex.getStackTrace.mkString("\n"))
            }
            connection ! Abort
          } else fail(ex)
        }
      })

    /** Fail stage and report to localAddressPromise if still possible */
    private def fail(ex: Throwable): Unit = {
      reportExceptionToPromise(ex)
      failStage(ex)
    }
    private def reportExceptionToPromise(ex: Throwable): Unit =
      role match {
        case Outbound(_, _, localAddressPromise, _) =>
          // Fail if has not been completed with an address earlier
          localAddressPromise.tryFailure(ex)
        case _ => // do nothing...
      }

    override def postStop(): Unit = reportExceptionToPromise(new StreamTcpException("Connection failed."))

    writeBuffer = ByteString.empty
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class IncomingConnectionStage(
    connection: ActorRef,
    remoteAddress: InetSocketAddress,
    halfClose: Boolean,
    registerCallback: () => Unit)
    extends GraphStage[FlowShape[ByteString, ByteString]] {
  import TcpConnectionStage._

  private val hasBeenCreated = new AtomicBoolean(false)

  val bytesIn: Inlet[ByteString] = Inlet("IncomingTCP.in")
  val bytesOut: Outlet[ByteString] = Outlet("IncomingTCP.out")
  override def initialAttributes = Attributes.name("IncomingConnection")
  val shape: FlowShape[ByteString, ByteString] = FlowShape(bytesIn, bytesOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    throw new UnsupportedOperationException("Not used")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes, eagerMaterializer: Materializer) = {
    if (hasBeenCreated.get) throw new IllegalStateException("Cannot materialize an incoming connection Flow twice.")
    hasBeenCreated.set(true)

    (
      new TcpStreamLogic(
        shape,
        Inbound(connection, halfClose, registerCallback),
        inheritedAttributes,
        remoteAddress,
        eagerMaterializer),
      NotUsed)
  }

  override def toString = s"TCP-from($remoteAddress)"
}

/**
 * INTERNAL API
 */
@InternalApi private[stream] class OutgoingConnectionStage(
    manager: ActorRef,
    remoteAddress: InetSocketAddress,
    localAddress: Option[InetSocketAddress] = None,
    options: immutable.Iterable[SocketOption] = Nil,
    halfClose: Boolean = true,
    connectTimeout: Duration = Duration.Inf)
    extends GraphStageWithMaterializedValue[FlowShape[ByteString, ByteString], Future[StreamTcp.OutgoingConnection]] {
  import TcpConnectionStage._

  val bytesIn: Inlet[ByteString] = Inlet("OutgoingTCP.in")
  val bytesOut: Outlet[ByteString] = Outlet("OutgoingTCP.out")
  override def initialAttributes = Attributes.name("OutgoingConnection")
  val shape: FlowShape[ByteString, ByteString] = FlowShape(bytesIn, bytesOut)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, Future[OutgoingConnection]) =
    throw new UnsupportedOperationException("Not used")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes,
      eagerMaterializer: Materializer): (GraphStageLogic, Future[StreamTcp.OutgoingConnection]) = {
    // FIXME: A method like this would make soo much sense on Duration (i.e. toOption)
    val connTimeout = connectTimeout match {
      case x: FiniteDuration => Some(x)
      case _                 => None
    }

    val localAddressPromise = Promise[InetSocketAddress]()
    val logic = new TcpStreamLogic(
      shape,
      Outbound(
        manager,
        Connect(remoteAddress, localAddress, options, connTimeout, pullMode = true),
        localAddressPromise,
        halfClose),
      inheritedAttributes,
      remoteAddress,
      eagerMaterializer)

    (logic, localAddressPromise.future.map(OutgoingConnection(remoteAddress, _))(ExecutionContext.parasitic))
  }

  override def toString = s"TCP-to($remoteAddress)"
}

/** INTERNAL API */
@InternalApi private[akka] object TcpIdleTimeout {
  def apply(
      idleTimeout: FiniteDuration,
      remoteAddress: Option[InetSocketAddress]): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    val connectionToString = remoteAddress match {
      case Some(address) => s" on connection to [$address]"
      case _             => ""
    }

    val toNetTimeout: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
      BidiFlow.fromFlows(
        Flow[ByteString].mapError {
          case _: TimeoutException =>
            new TcpIdleTimeoutException(
              s"TCP idle-timeout encountered$connectionToString, no bytes passed in the last $idleTimeout",
              idleTimeout)
        },
        Flow[ByteString])
    val fromNetTimeout: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
      toNetTimeout.reversed // now the bottom flow transforms the exception, the top one doesn't (since that one is "fromNet")

    fromNetTimeout.atop(BidiFlow.bidirectionalIdleTimeout[ByteString, ByteString](idleTimeout)).atop(toNetTimeout)
  }
}
