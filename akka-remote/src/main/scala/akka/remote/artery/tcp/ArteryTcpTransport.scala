/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package tcp

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.ConfigurationException
import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.remote.RemoteActorRefProvider
import akka.remote.RemoteLogMarker
import akka.remote.RemoteTransportException
import akka.remote.artery.Decoder.InboundCompressionAccess
import akka.remote.artery.compress._
import akka.stream.Attributes
import akka.stream.Attributes.LogLevels
import akka.stream.IgnoreComplete
import akka.stream.KillSwitches
import akka.stream.Materializer
import akka.stream.RestartSettings
import akka.stream.SharedKillSwitch
import akka.stream.SinkShape
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergeHub
import akka.stream.scaladsl.Partition
import akka.stream.scaladsl.RestartFlow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.util.{ ByteString, OptionVal }

/**
 * INTERNAL API
 */
private[remote] object ArteryTcpTransport {

  private val successUnit = Success(())

  def optionToTry(opt: Option[Throwable]): Try[Unit] = opt match {
    case None    => successUnit
    case Some(t) => Failure(t)
  }
}

/**
 * INTERNAL API
 */
private[remote] class ArteryTcpTransport(
    _system: ExtendedActorSystem,
    _provider: RemoteActorRefProvider,
    tlsEnabled: Boolean)
    extends ArteryTransport(_system, _provider) {
  import ArteryTcpTransport._
  import ArteryTransport._

  override type LifeCycle = NotUsed

  // may change when inbound streams are restarted
  @volatile private var inboundKillSwitch: SharedKillSwitch = KillSwitches.shared("inboundKillSwitch")
  // may change when inbound streams are restarted
  @volatile private var serverBinding: Option[ServerBinding] = None
  private val firstConnectionFlow = Promise[Flow[ByteString, ByteString, NotUsed]]()
  @volatile private var inboundConnectionFlow: Future[Flow[ByteString, ByteString, NotUsed]] =
    firstConnectionFlow.future

  private val sslEngineProvider: OptionVal[SSLEngineProvider] =
    if (tlsEnabled) {
      system.settings.setup.get[SSLEngineProviderSetup] match {
        case Some(p) =>
          OptionVal.Some(p.sslEngineProvider(system))
        case None =>
          // load from config
          OptionVal.Some(
            system.dynamicAccess
              .createInstanceFor[SSLEngineProvider](
                settings.SSLEngineProviderClassName,
                List((classOf[ActorSystem], system)))
              .recover {
                case e =>
                  throw new ConfigurationException(
                    s"Could not create SSLEngineProvider [${settings.SSLEngineProviderClassName}]",
                    e)
              }
              .get)
      }
    } else OptionVal.None

  override protected def startTransport(): Unit = {
    // nothing specific here
  }

  override protected def outboundTransportSink(
      outboundContext: OutboundContext,
      streamId: Int,
      bufferPool: EnvelopeBufferPool): Sink[EnvelopeBuffer, Future[Done]] = {

    val host = outboundContext.remoteAddress.host.get
    val port = outboundContext.remoteAddress.port.get
    val remoteAddress = InetSocketAddress.createUnresolved(host, port)

    def connectionFlow: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = {
      val localAddress = settings.Advanced.Tcp.OutboundClientHostname match {
        case None                 => None
        case Some(clientHostname) => Some(new InetSocketAddress(clientHostname, 0))
      }
      if (tlsEnabled) {
        val sslProvider = sslEngineProvider.get
        Tcp(system).outgoingConnectionWithTls(
          remoteAddress,
          createSSLEngine = () => sslProvider.createClientSSLEngine(host, port),
          localAddress,
          options = Nil,
          connectTimeout = settings.Advanced.Tcp.ConnectionTimeout,
          idleTimeout = Duration.Inf,
          verifySession = session => optionToTry(sslProvider.verifyClientSession(host, session)),
          closing = IgnoreComplete)
      } else {
        Tcp(system).outgoingConnection(
          remoteAddress,
          localAddress,
          halfClose = true, // issue https://github.com/akka/akka-core/issues/24392 if set to false
          connectTimeout = settings.Advanced.Tcp.ConnectionTimeout)
      }
    }

    def connectionFlowWithRestart: Flow[ByteString, ByteString, NotUsed] = {
      val restartCount = new AtomicInteger(0)

      def logConnect(): Unit = {
        if (log.isDebugEnabled)
          log.debug(
            RemoteLogMarker.connect(
              outboundContext.remoteAddress,
              outboundContext.associationState.uniqueRemoteAddress().map(_.uid)),
            "Outbound connection opened to [{}]",
            outboundContext.remoteAddress)
      }

      def logDisconnected(): Unit = {
        if (log.isDebugEnabled)
          log.debug(
            RemoteLogMarker.disconnected(
              outboundContext.remoteAddress,
              outboundContext.associationState.uniqueRemoteAddress().map(_.uid)),
            "Outbound connection closed to [{}]",
            outboundContext.remoteAddress)
      }

      val flowFactory = () => {
        val onFailureLogLevel = if (restartCount.incrementAndGet() == 1) Logging.WarningLevel else Logging.DebugLevel

        def flow(controlIdleKillSwitch: OptionVal[SharedKillSwitch]) =
          Flow[ByteString]
            .via(Flow.lazyFlow(() => {
              // only open the actual connection if any new messages are sent
              logConnect()
              RemotingFlightRecorder.tcpOutboundConnected(outboundContext.remoteAddress, streamName(streamId))
              if (controlIdleKillSwitch.isDefined)
                outboundContext.asInstanceOf[Association].setControlIdleKillSwitch(controlIdleKillSwitch)

              Flow[ByteString].prepend(Source.single(TcpFraming.encodeConnectionHeader(streamId))).via(connectionFlow)
            }))
            .mapError {
              case ArteryTransport.ShutdownSignal => ArteryTransport.ShutdownSignal
              case e =>
                logDisconnected()
                e
            }
            .recoverWithRetries(1, { case ArteryTransport.ShutdownSignal => Source.empty })
            .log(name = s"outbound connection to [${outboundContext.remoteAddress}], ${streamName(streamId)} stream")
            .addAttributes(Attributes.logLevels(onElement = LogLevels.Off, onFailure = onFailureLogLevel))

        if (streamId == ControlStreamId) {
          // must replace the KillSwitch when restarted
          val controlIdleKillSwitch = KillSwitches.shared("outboundControlStreamIdleKillSwitch")
          Flow[ByteString].via(controlIdleKillSwitch.flow).via(flow(OptionVal.Some(controlIdleKillSwitch)))
        } else {
          flow(OptionVal.None)
        }
      }

      val maxRestarts = if (streamId == ControlStreamId) Int.MaxValue else 3
      // Restart of inner connection part important in control stream, since system messages
      // are buffered and resent from the outer SystemMessageDelivery stage. No maxRestarts limit for control
      // stream. For message stream it's best effort retry a few times.
      RestartFlow
        .withBackoff[ByteString, ByteString](
          RestartSettings(settings.Advanced.OutboundRestartBackoff, settings.Advanced.OutboundRestartBackoff * 5, 0.1)
            .withMaxRestarts(maxRestarts, settings.Advanced.OutboundRestartBackoff))(flowFactory)
        // silence "Restarting graph due to failure" logging by RestartFlow
        .addAttributes(Attributes.logLevels(onFailure = LogLevels.Off))

    }

    Flow[EnvelopeBuffer]
      .map { env =>
        val size = env.byteBuffer.limit()
        RemotingFlightRecorder.tcpOutboundSent(size)

        // TODO Possible performance improvement, could we reduce the copying of bytes?
        val bytes = ByteString(env.byteBuffer)
        bufferPool.release(env)

        TcpFraming.encodeFrameHeader(size) ++ bytes
      }
      .via(connectionFlowWithRestart)
      .map(_ =>
        throw new IllegalStateException(
          s"Unexpected incoming bytes in outbound connection to [${outboundContext.remoteAddress}]"))
      .toMat(Sink.ignore)(Keep.right)
  }

  override protected def bindInboundStreams(): (Int, Int) = {
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = materializer

    val bindHost = settings.Bind.Hostname
    val bindPort = settings.Bind.Port

    val connectionSource: Source[Tcp.IncomingConnection, Future[ServerBinding]] =
      if (tlsEnabled) {
        val sslProvider = sslEngineProvider.get
        Tcp(system).bindWithTls(
          interface = bindHost,
          port = bindPort,
          createSSLEngine = () => sslProvider.createServerSSLEngine(bindHost, bindPort),
          backlog = Tcp.defaultBacklog,
          options = Nil,
          idleTimeout = Duration.Inf,
          verifySession = session => optionToTry(sslProvider.verifyServerSession(bindHost, session)),
          closing = IgnoreComplete)
      } else {
        Tcp(system).bind(interface = bindHost, port = bindPort, halfClose = false)
      }

    val binding = serverBinding match {
      case None =>
        val binding = connectionSource
          .to(Sink.foreach { connection =>
            RemotingFlightRecorder.tcpInboundConnected(connection.remoteAddress)
            inboundConnectionFlow.map(connection.handleWith(_))(sys.dispatcher)
          })
          .run()
          .recoverWith {
            case e =>
              Future.failed(
                new RemoteTransportException(
                  s"Failed to bind TCP to [$bindHost:$bindPort] due to: " +
                  e.getMessage,
                  e))
          }(ExecutionContext.parasitic)

        // only on initial startup, when ActorSystem is starting
        val b = Await.result(binding, settings.Bind.BindTimeout)
        RemotingFlightRecorder.tcpInboundBound(bindHost, b.localAddress)
        b
      case Some(binding) =>
        // already bound, when restarting
        binding
    }
    serverBinding = Some(binding)
    if (settings.Canonical.Port == 0)
      (binding.localAddress.getPort, binding.localAddress.getPort)
    else
      (settings.Canonical.Port, binding.localAddress.getPort)
  }

  override protected def runInboundStreams(port: Int, bindPort: Int): Unit = {
    // Design note: The design of how to run the inbound streams are influenced by the original design
    // for the Aeron streams, and there we can only have one single inbound since everything comes in
    // via the single AeronSource.
    //
    // For TCP we could materialize the inbound streams for each inbound connection, i.e. running many
    // completely separate inbound streams. Each would still have to include all 3 control, ordinary,
    // large parts for each connection even though only one is used by a specific connection. Unless
    // we can dynamically choose what to materialize based on the `streamId` (as a first byte, or
    // in first frame of the connection), which is complicated.
    //
    // However, this would be make the design for Aeron and TCP more different and more things might
    // have to be changed, such as compression advertisements and materialized values. Number of
    // inbound streams would be dynamic, and so on.

    // These streams are always running, only one instance of each inbound stream, and then the inbound connections
    // are attached to these via a MergeHub.
    val (controlStream, controlStreamCompleted) = runInboundControlStream()
    val (ordinaryMessagesStream, ordinaryMessagesStreamCompleted) = runInboundOrdinaryMessagesStream()
    val (largeMessagesStream, largeMessagesStreamCompleted) = {
      if (largeMessageChannelEnabled)
        runInboundLargeMessagesStream()
      else
        (
          Flow[EnvelopeBuffer]
            .map(_ => log.warning("Dropping large message, missing large-message-destinations configuration."))
            .to(Sink.ignore),
          Promise[Done]().future) // never completed, not enabled
    }

    // An inbound connection will only use one of the control, ordinary or large streams, but we have to
    // attach it to all and select via Partition and the streamId in the frame header. Conceptually it
    // would have been better to send the streamId as one single first byte for a new connection and
    // decide where to attach it based on that byte. Then the streamId wouldn't have to be sent in each
    // frame. That was not chosen because it is more complicated to implement and might have more runtime
    // overhead.
    val inboundStream = Sink.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val partition = b.add(Partition[EnvelopeBuffer](3, env => {
        env.streamId match {
          case OrdinaryStreamId => 1
          case ControlStreamId  => 0
          case LargeStreamId    => 2
          case other            => throw new IllegalArgumentException(s"Unexpected streamId [$other]")
        }
      }))
      partition.out(0) ~> controlStream
      partition.out(1) ~> ordinaryMessagesStream
      partition.out(2) ~> largeMessagesStream
      SinkShape(partition.in)
    })

    // If something in the inboundConnectionFlow fails, e.g. framing, the connection will be torn down,
    // but other parts of the inbound streams don't have to be restarted.
    val maxFrameLength =
      if (largeMessageChannelEnabled)
        math.max(settings.Advanced.MaximumFrameSize, settings.Advanced.MaximumLargeFrameSize)
      else
        settings.Advanced.MaximumFrameSize
    val newInboundConnectionFlow = {
      Flow[ByteString]
        .via(inboundKillSwitch.flow)
        // must create new FlightRecorder event sink for each connection because they can't be shared
        .via(new TcpFraming(maxFrameLength))
        .alsoTo(inboundStream)
        .filter(_ => false) // don't send back anything in this TCP socket
        .map(_ => ByteString.empty) // make it a Flow[ByteString] again
    }
    firstConnectionFlow.trySuccess(newInboundConnectionFlow)
    inboundConnectionFlow = Future.successful(newInboundConnectionFlow)

    // Failures in any of the inbound streams should be extremely rare, probably an unforeseen accident.
    // Tear down everything and start over again. Inbound streams are "stateless" so that should be fine.
    // Tested in SurviveInboundStreamRestartWithCompressionInFlightSpec
    implicit val ec: ExecutionContext = materializer.executionContext
    val completed = Future.firstCompletedOf(
      List(controlStreamCompleted, ordinaryMessagesStreamCompleted, largeMessagesStreamCompleted))
    val restart = () => {
      inboundKillSwitch.shutdown()
      inboundKillSwitch = KillSwitches.shared("inboundKillSwitch")

      val allStopped: Future[Done] = for {
        _ <- controlStreamCompleted.recover { case _          => Done }
        _ <- ordinaryMessagesStreamCompleted.recover { case _ => Done }
        _ <- if (largeMessageChannelEnabled)
          largeMessagesStreamCompleted.recover { case _ => Done } else Future.successful(Done)
      } yield Done
      allStopped.foreach(_ => runInboundStreams(port, bindPort))
    }

    attachInboundStreamRestart("Inbound streams", completed, restart)
  }

  private def runInboundControlStream(): (Sink[EnvelopeBuffer, NotUsed], Future[Done]) = {
    if (isShutdown) throw ArteryTransport.ShuttingDown

    val (hub, ctrl, completed) =
      MergeHub
        .source[EnvelopeBuffer]
        .addAttributes(Attributes.logLevels(onFailure = LogLevels.Off))
        .via(inboundKillSwitch.flow)
        .via(inboundFlow(settings, NoInboundCompressions))
        .toMat(inboundControlSink)({ case (a, (c, d)) => (a, c, d) })
        .run()(controlMaterializer)
    attachControlMessageObserver(ctrl)
    updateStreamMatValues(completed)

    (hub, completed)
  }

  private def runInboundOrdinaryMessagesStream(): (Sink[EnvelopeBuffer, NotUsed], Future[Done]) = {
    if (isShutdown) throw ArteryTransport.ShuttingDown

    val (inboundHub: Sink[EnvelopeBuffer, NotUsed], inboundCompressionAccess, completed) =
      if (inboundLanes == 1) {
        MergeHub
          .source[EnvelopeBuffer]
          .addAttributes(Attributes.logLevels(onFailure = LogLevels.Off))
          .via(inboundKillSwitch.flow)
          .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)
          .toMat(inboundSink(envelopeBufferPool))({ case ((a, b), c) => (a, b, c) })
          .run()(materializer)

      } else {
        // TODO perhaps a few more things can be extracted and DRY with AeronUpdTransport.runInboundOrdinaryMessagesStream
        val laneKillSwitch = KillSwitches.shared("laneKillSwitch")
        val laneSource: Source[InboundEnvelope, (Sink[EnvelopeBuffer, NotUsed], InboundCompressionAccess)] =
          MergeHub
            .source[EnvelopeBuffer]
            .addAttributes(Attributes.logLevels(onFailure = LogLevels.Off))
            .via(inboundKillSwitch.flow)
            .via(laneKillSwitch.flow)
            .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)
            .via(Flow.fromGraph(new DuplicateHandshakeReq(inboundLanes, this, system, envelopeBufferPool)))
            .via(Flow.fromGraph(new DuplicateFlush(inboundLanes, system, envelopeBufferPool)))

        val (inboundHub, compressionAccess, laneHub) =
          laneSource
            .toMat(
              Sink.fromGraph(
                new FixedSizePartitionHub[InboundEnvelope](
                  inboundLanePartitioner,
                  inboundLanes,
                  settings.Advanced.InboundHubBufferSize)))({
              case ((a, b), c) => (a, b, c)
            })
            .run()(materializer)

        val lane = inboundSink(envelopeBufferPool)
        val completedValues: Vector[Future[Done]] =
          (0 until inboundLanes).iterator
            .map { _ =>
              laneHub.toMat(lane)(Keep.right).run()(materializer)
            }
            .to(immutable.Vector)

        implicit val ec = system.dispatchers.internalDispatcher

        // tear down the upstream hub part if downstream lane fails
        // lanes are not completed with success by themselves so we don't have to care about onSuccess
        Future.firstCompletedOf(completedValues).failed.foreach { reason =>
          laneKillSwitch.abort(reason)
        }
        val allCompleted = Future.sequence(completedValues).map(_ => Done)

        (inboundHub, compressionAccess, allCompleted)
      }

    setInboundCompressionAccess(inboundCompressionAccess)

    updateStreamMatValues(completed)

    (inboundHub, completed)
  }

  private def runInboundLargeMessagesStream(): (Sink[EnvelopeBuffer, NotUsed], Future[Done]) = {
    if (isShutdown) throw ArteryTransport.ShuttingDown

    val (hub, completed) =
      MergeHub
        .source[EnvelopeBuffer]
        .addAttributes(Attributes.logLevels(onFailure = LogLevels.Off))
        .via(inboundKillSwitch.flow)
        .via(inboundLargeFlow(settings))
        .toMat(inboundSink(largeEnvelopeBufferPool))(Keep.both)
        .run()(materializer)

    updateStreamMatValues(completed)

    (hub, completed)
  }

  private def updateStreamMatValues(completed: Future[Done]): Unit = {
    implicit val ec: ExecutionContext = materializer.executionContext
    updateStreamMatValues(
      ControlStreamId,
      InboundStreamMatValues[NotUsed](NotUsed, completed.recover { case _ => Done }))
  }

  override protected def shutdownTransport(): Future[Done] = {
    implicit val ec = system.dispatchers.internalDispatcher
    inboundKillSwitch.shutdown()
    unbind().map { _ =>
      RemotingFlightRecorder.transportStopped()
      Done
    }
  }

  private def unbind(): Future[Done] = {
    serverBinding match {
      case Some(binding) =>
        implicit val ec = system.dispatchers.internalDispatcher
        for {
          _ <- binding.unbind()
        } yield {
          RemotingFlightRecorder.tcpInboundUnbound(localAddress)
          Done
        }
      case None =>
        Future.successful(Done)
    }
  }

}
