/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package aeron

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.util.control.NonFatal

import io.aeron.Aeron
import io.aeron.AvailableImageHandler
import io.aeron.CncFileDescriptor
import io.aeron.CommonContext
import io.aeron.Image
import io.aeron.UnavailableImageHandler
import io.aeron.driver.MediaDriver
import io.aeron.driver.ThreadingMode
import io.aeron.exceptions.ConductorServiceTimeoutException
import io.aeron.exceptions.DriverTimeoutException
import io.aeron.status.ChannelEndpointStatus
import org.agrona.DirectBuffer
import org.agrona.ErrorHandler
import org.agrona.IoUtil
import org.agrona.concurrent.BackoffIdleStrategy
import org.agrona.concurrent.status.CountersReader.MetaData

import akka.Done
import akka.actor.Address
import akka.actor.Cancellable
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.remote.RemoteActorRefProvider
import akka.remote.RemoteTransportException
import akka.remote.artery.compress._
import akka.stream.KillSwitches
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
private[remote] class ArteryAeronUdpTransport(_system: ExtendedActorSystem, _provider: RemoteActorRefProvider)
    extends ArteryTransport(_system, _provider) {
  import AeronSource.AeronLifecycle
  import ArteryTransport._
  import Decoder.InboundCompressionAccess

  override type LifeCycle = AeronLifecycle

  private[this] val mediaDriver = new AtomicReference[Option[MediaDriver]](None)
  @volatile private[this] var aeron: Aeron = _
  @volatile private[this] var aeronCounterTask: Cancellable = _
  @volatile private[this] var aeronErrorLogTask: Cancellable = _
  @volatile private[this] var aeronErrorLog: AeronErrorLog = _

  private val taskRunner = new TaskRunner(system, settings.Advanced.Aeron.IdleCpuLevel)

  private def outboundChannel(a: Address) = s"aeron:udp?endpoint=${a.host.get}:${a.port.get}"

  override protected def startTransport(): Unit = {
    startMediaDriver()
    startAeron()
    startAeronErrorLog()
    RemotingFlightRecorder.transportAeronErrorLogStarted()
    if (settings.Advanced.Aeron.LogAeronCounters) {
      startAeronCounterLog()
    }
    taskRunner.start()
    RemotingFlightRecorder.transportTaskRunnerStarted()
  }

  private def startMediaDriver(): Unit = {
    if (settings.Advanced.Aeron.EmbeddedMediaDriver) {
      val driverContext = new MediaDriver.Context
      if (settings.Advanced.Aeron.AeronDirectoryName.nonEmpty) {
        driverContext.aeronDirectoryName(settings.Advanced.Aeron.AeronDirectoryName)
      } else {
        // create a random name but include the actor system name for easier debugging
        val uniquePart = UUID.randomUUID().toString
        val randomName = s"${CommonContext.getAeronDirectoryName}${File.separator}${system.name}-$uniquePart"
        driverContext.aeronDirectoryName(randomName)
      }
      driverContext.clientLivenessTimeoutNs(settings.Advanced.Aeron.ClientLivenessTimeout.toNanos)
      driverContext.publicationUnblockTimeoutNs(settings.Advanced.Aeron.PublicationUnblockTimeout.toNanos)
      driverContext.imageLivenessTimeoutNs(settings.Advanced.Aeron.ImageLivenessTimeout.toNanos)
      driverContext.driverTimeoutMs(settings.Advanced.Aeron.DriverTimeout.toMillis)

      val idleCpuLevel = settings.Advanced.Aeron.IdleCpuLevel
      if (idleCpuLevel == 10) {
        driverContext
          .threadingMode(ThreadingMode.DEDICATED)
          .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
          .receiverIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .senderIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .conductorThreadFactory(system.threadFactory)
          .receiverThreadFactory(system.threadFactory)
          .senderThreadFactory(system.threadFactory)
      } else if (idleCpuLevel == 1) {
        driverContext
          .threadingMode(ThreadingMode.SHARED)
          .sharedIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .sharedThreadFactory(system.threadFactory)
      } else if (idleCpuLevel <= 7) {
        driverContext
          .threadingMode(ThreadingMode.SHARED_NETWORK)
          .sharedNetworkIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .sharedNetworkThreadFactory(system.threadFactory)
          .conductorThreadFactory(system.threadFactory)
      } else {
        driverContext
          .threadingMode(ThreadingMode.DEDICATED)
          .receiverIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .senderIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .receiverThreadFactory(system.threadFactory)
          .senderThreadFactory(system.threadFactory)
          .conductorThreadFactory(system.threadFactory)
      }

      val driver = MediaDriver.launchEmbedded(driverContext)
      log.info("Started embedded media driver in directory [{}]", driver.aeronDirectoryName)
      RemotingFlightRecorder.transportMediaDriverStarted(driver.aeronDirectoryName())
      if (!mediaDriver.compareAndSet(None, Some(driver))) {
        throw new IllegalStateException("media driver started more than once")
      }
    }
  }

  private def aeronDir: String = mediaDriver.get match {
    case Some(driver) => driver.aeronDirectoryName
    case None         => settings.Advanced.Aeron.AeronDirectoryName
  }

  private def stopMediaDriver(): Unit = {
    // make sure we only close the driver once or we will crash the JVM
    val maybeDriver = mediaDriver.getAndSet(None)
    maybeDriver.foreach { driver =>
      log.info("Stopping embedded media driver in directory [{}]", driver.aeronDirectoryName)
      // this is only for embedded media driver
      try driver.close()
      catch {
        case NonFatal(e) =>
          // don't think driver.close will ever throw, but just in case
          log.warning("Couldn't close Aeron embedded media driver due to [{}]", e)
      }

      try {
        if (settings.Advanced.Aeron.DeleteAeronDirectory) {
          IoUtil.delete(new File(driver.aeronDirectoryName), false)
          RemotingFlightRecorder.transportMediaFileDeleted()
        }
      } catch {
        case NonFatal(e) =>
          log.warning(
            "Couldn't delete Aeron embedded media driver files in [{}] due to [{}]",
            driver.aeronDirectoryName,
            e)
      }
    }
  }

  // TODO: Add FR events
  private def startAeron(): Unit = {
    val ctx = new Aeron.Context

    ctx.driverTimeoutMs(settings.Advanced.Aeron.DriverTimeout.toMillis)
    ctx.threadFactory(system.threadFactory)

    ctx.availableImageHandler(new AvailableImageHandler {
      override def onAvailableImage(img: Image): Unit = {
        if (log.isDebugEnabled)
          log.debug(s"onAvailableImage from ${img.sourceIdentity} session ${img.sessionId}")
      }
    })
    ctx.unavailableImageHandler(new UnavailableImageHandler {
      override def onUnavailableImage(img: Image): Unit = {
        if (log.isDebugEnabled)
          log.debug(s"onUnavailableImage from ${img.sourceIdentity} session ${img.sessionId}")

        // freeSessionBuffer in AeronSource FragmentAssembler
        streamMatValues.get.valuesIterator.foreach {
          case InboundStreamMatValues(resourceLife, _) =>
            resourceLife.onUnavailableImage(img.sessionId)
        }
      }
    })

    ctx.errorHandler(new ErrorHandler {
      private val fatalErrorOccured = new AtomicBoolean

      override def onError(cause: Throwable): Unit = {
        cause match {
          case e: ConductorServiceTimeoutException => handleFatalError(e)
          case e: DriverTimeoutException           => handleFatalError(e)
          case _: AeronTerminated                  => // already handled, via handleFatalError
          case _ =>
            log.error(cause, s"Aeron error, $cause")
        }
      }

      private def handleFatalError(cause: Throwable): Unit = {
        if (fatalErrorOccured.compareAndSet(false, true)) {
          if (!isShutdown) {
            log.error(
              cause,
              "Fatal Aeron error {}. Have to terminate ActorSystem because it lost contact with the " +
              "{} Aeron media driver. Possible configuration properties to mitigate the problem are " +
              "'client-liveness-timeout' or 'driver-timeout'. {}",
              Logging.simpleName(cause),
              if (settings.Advanced.Aeron.EmbeddedMediaDriver) "embedded" else "external",
              cause)
            taskRunner.stop()
            aeronErrorLogTask.cancel()
            if (settings.Advanced.Aeron.LogAeronCounters) aeronCounterTask.cancel()
            system.terminate()
            throw new AeronTerminated(cause)
          }
        } else
          throw new AeronTerminated(cause)
      }
    })

    ctx.aeronDirectoryName(aeronDir)
    aeron = Aeron.connect(ctx)
  }

  private def blockUntilChannelActive(): Unit = {
    val aeronLifecyle = streamMatValues.get()(ControlStreamId).lifeCycle

    val waitInterval = 200
    val retries = math.max(1, settings.Bind.BindTimeout.toMillis / waitInterval)
    retry(retries)

    @tailrec def retry(retries: Long): Unit = {
      val status = Await.result(aeronLifecyle.channelEndpointStatus(), settings.Bind.BindTimeout)
      if (status == ChannelEndpointStatus.ACTIVE) {
        log.debug("Inbound channel is now active")
      } else if (status == ChannelEndpointStatus.ERRORED) {
        aeronErrorLog.logErrors(log, 0L)
        stopMediaDriver()
        throw new RemoteTransportException("Inbound Aeron channel is in errored state. See Aeron logs for details.")
      } else if (status == ChannelEndpointStatus.INITIALIZING && retries > 0) {
        Thread.sleep(waitInterval)
        retry(retries - 1)
      } else {
        aeronErrorLog.logErrors(log, 0L)
        stopMediaDriver()
        throw new RemoteTransportException("Timed out waiting for Aeron transport to bind. See Aeoron logs.")
      }
    }
  }

  // TODO Add FR Events
  private def startAeronErrorLog(): Unit = {
    aeronErrorLog = new AeronErrorLog(new File(aeronDir, CncFileDescriptor.CNC_FILE), log)
    val lastTimestamp = new AtomicLong(0L)
    implicit val ec = system.dispatchers.internalDispatcher
    aeronErrorLogTask = system.scheduler.scheduleWithFixedDelay(3.seconds, 5.seconds) { () =>
      if (!isShutdown) {
        val newLastTimestamp = aeronErrorLog.logErrors(log, lastTimestamp.get)
        lastTimestamp.set(newLastTimestamp + 1)
      }
    }
  }

  private def startAeronCounterLog(): Unit = {
    implicit val ec = system.dispatchers.internalDispatcher
    aeronCounterTask = system.scheduler.scheduleWithFixedDelay(5.seconds, 5.seconds) { () =>
      if (!isShutdown && log.isDebugEnabled) {
        aeron.countersReader.forEach(new MetaData() {
          def accept(counterId: Int, typeId: Int, keyBuffer: DirectBuffer, label: String): Unit = {
            val value = aeron.countersReader().getCounterValue(counterId)
            log.debug("Aeron Counter {}: {} {}]", counterId, value, label)
          }
        })
      }
    }
  }

  override protected def outboundTransportSink(
      outboundContext: OutboundContext,
      streamId: Int,
      bufferPool: EnvelopeBufferPool): Sink[EnvelopeBuffer, Future[Done]] = {
    val giveUpAfter =
      if (streamId == ControlStreamId) settings.Advanced.GiveUpSystemMessageAfter
      else settings.Advanced.Aeron.GiveUpMessageAfter
    // TODO: Note that the AssociationState.controlStreamIdleKillSwitch in control stream is not used for the
    // Aeron transport. Would be difficult to handle the Future[Done] materialized value.
    // If we want to stop for Aeron also it is probably easier to stop the publication inside the
    // AeronSink, i.e. not using a KillSwitch.
    Sink.fromGraph(
      new AeronSink(
        outboundChannel(outboundContext.remoteAddress),
        streamId,
        aeron,
        taskRunner,
        bufferPool,
        giveUpAfter))
  }

  private def aeronSource(
      streamId: Int,
      pool: EnvelopeBufferPool,
      inboundChannel: String): Source[EnvelopeBuffer, AeronSource.AeronLifecycle] =
    Source.fromGraph(new AeronSource(inboundChannel, streamId, aeron, taskRunner, pool, aeronSourceSpinningStrategy))

  private def aeronSourceSpinningStrategy: Int =
    if (settings.Advanced.InboundLanes > 1 || // spinning was identified to be the cause of massive slowdowns with multiple lanes, see #21365
        settings.Advanced.Aeron.IdleCpuLevel < 5) 0 // also don't spin for small IdleCpuLevels
    else 50 * settings.Advanced.Aeron.IdleCpuLevel - 240

  override protected def bindInboundStreams(): (Int, Int) = {
    (settings.Canonical.Port, settings.Bind.Port) match {
      case (0, 0) =>
        val p = autoSelectPort(settings.Bind.Hostname)
        (p, p)
      case (0, _) =>
        (settings.Bind.Port, settings.Bind.Port)
      case (_, 0) =>
        (settings.Canonical.Port, autoSelectPort(settings.Bind.Hostname))
      case _ =>
        (settings.Canonical.Port, settings.Bind.Port)
    }
  }

  override protected def runInboundStreams(port: Int, bindPort: Int): Unit = {
    val inboundChannel = s"aeron:udp?endpoint=${settings.Bind.Hostname}:$bindPort"

    runInboundControlStream(inboundChannel)
    runInboundOrdinaryMessagesStream(inboundChannel)

    if (largeMessageChannelEnabled) {
      runInboundLargeMessagesStream(inboundChannel)
    }
    blockUntilChannelActive()
  }

  private def runInboundControlStream(inboundChannel: String): Unit = {
    if (isShutdown) throw ShuttingDown

    val (resourceLife, ctrl, completed) =
      aeronSource(ControlStreamId, envelopeBufferPool, inboundChannel)
        .via(inboundFlow(settings, NoInboundCompressions))
        .toMat(inboundControlSink)({ case (a, (c, d)) => (a, c, d) })
        .run()(controlMaterializer)

    attachControlMessageObserver(ctrl)

    updateStreamMatValues(ControlStreamId, resourceLife, completed)
    attachInboundStreamRestart("Inbound control stream", completed, () => runInboundControlStream(inboundChannel))
  }

  private def runInboundOrdinaryMessagesStream(inboundChannel: String): Unit = {
    if (isShutdown) throw ShuttingDown

    val (resourceLife, inboundCompressionAccess, completed) =
      if (inboundLanes == 1) {
        aeronSource(OrdinaryStreamId, envelopeBufferPool, inboundChannel)
          .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)
          .toMat(inboundSink(envelopeBufferPool))({ case ((a, b), c) => (a, b, c) })
          .run()(materializer)

      } else {
        val laneKillSwitch = KillSwitches.shared("laneKillSwitch")
        val laneSource: Source[InboundEnvelope, (AeronLifecycle, InboundCompressionAccess)] =
          aeronSource(OrdinaryStreamId, envelopeBufferPool, inboundChannel)
            .via(laneKillSwitch.flow)
            .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)
            .via(Flow.fromGraph(new DuplicateHandshakeReq(inboundLanes, this, system, envelopeBufferPool)))
            .via(Flow.fromGraph(new DuplicateFlush(inboundLanes, system, envelopeBufferPool)))

        val (resourceLife, compressionAccess, laneHub) =
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

        (resourceLife, compressionAccess, allCompleted)
      }

    setInboundCompressionAccess(inboundCompressionAccess)

    updateStreamMatValues(OrdinaryStreamId, resourceLife, completed)
    attachInboundStreamRestart(
      "Inbound message stream",
      completed,
      () => runInboundOrdinaryMessagesStream(inboundChannel))
  }

  private def runInboundLargeMessagesStream(inboundChannel: String): Unit = {
    if (isShutdown) throw ShuttingDown

    val (resourceLife, completed) = aeronSource(LargeStreamId, largeEnvelopeBufferPool, inboundChannel)
      .via(inboundLargeFlow(settings))
      .toMat(inboundSink(largeEnvelopeBufferPool))(Keep.both)
      .run()(materializer)

    updateStreamMatValues(LargeStreamId, resourceLife, completed)
    attachInboundStreamRestart(
      "Inbound large message stream",
      completed,
      () => runInboundLargeMessagesStream(inboundChannel))
  }

  private def updateStreamMatValues(
      streamId: Int,
      aeronSourceLifecycle: AeronSource.AeronLifecycle,
      completed: Future[Done]): Unit = {
    implicit val ec = materializer.executionContext
    updateStreamMatValues(streamId, InboundStreamMatValues[AeronLifecycle](aeronSourceLifecycle, completed.recover {
      case _ => Done
    }))
  }

  override protected def shutdownTransport(): Future[Done] = {
    taskRunner
      .stop()
      .map { _ =>
        RemotingFlightRecorder.transportStopped()
        if (aeronErrorLogTask != null) {
          aeronErrorLogTask.cancel()
          RemotingFlightRecorder.transportAeronErrorLogTaskStopped()
        }
        if (aeron != null) aeron.close()
        if (aeronErrorLog != null) aeronErrorLog.close()
        if (mediaDriver.get.isDefined) stopMediaDriver()

        Done
      }(system.dispatchers.internalDispatcher)
  }

  def autoSelectPort(hostname: String): Int = {
    import java.net.InetSocketAddress
    import java.nio.channels.DatagramChannel

    val socket = DatagramChannel.open().socket()
    socket.bind(new InetSocketAddress(hostname, 0))
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
