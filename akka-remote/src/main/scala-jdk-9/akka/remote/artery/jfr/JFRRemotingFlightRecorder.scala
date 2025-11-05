/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.remote.artery.jfr

import java.net.InetSocketAddress

import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.remote.UniqueAddress
import akka.remote.artery.RemotingFlightRecorder

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class JFRRemotingFlightRecorder() extends RemotingFlightRecorder {
  override def transportMediaDriverStarted(directoryName: String): Unit =
    new TransportMediaDriverStarted(directoryName).commit()

  override def transportStarted(): Unit =
    new TransportStarted().commit()

  override def transportAeronErrorLogStarted(): Unit =
    new TransportAeronErrorLogStarted().commit()

  override def transportTaskRunnerStarted(): Unit =
    new TransportTaskRunnerStarted().commit()

  override def transportUniqueAddressSet(uniqueAddress: UniqueAddress): Unit =
    new TransportUniqueAddressSet(uniqueAddress).commit()

  override def transportMaterializerStarted(): Unit =
    new TransportMaterializerStarted().commit()

  override def transportStartupFinished(): Unit =
    new TransportStartupFinished().commit()

  override def transportKillSwitchPulled(): Unit =
    new TransportKillSwitchPulled().commit()

  override def transportStopped(): Unit =
    new TransportStopped().commit()

  override def transportAeronErrorLogTaskStopped(): Unit =
    new TransportAeronErrorLogTaskStopped().commit()

  override def transportMediaFileDeleted(): Unit =
    new TransportMediaFileDeleted().commit()

  override def transportSendQueueOverflow(queueIndex: Int): Unit =
    new TransportSendQueueOverflow(queueIndex).commit()

  override def transportStopIdleOutbound(remoteAddress: Address, queueIndex: Int): Unit =
    new TransportStopIdleOutbound(remoteAddress, queueIndex).commit()

  override def transportQuarantined(remoteAddress: Address, uid: Long): Unit =
    new TransportQuarantined(remoteAddress, uid).commit()

  override def transportRemoveQuarantined(remoteAddress: Address): Unit =
    new TransportRemoveQuarantined(remoteAddress).commit()

  override def transportRestartOutbound(remoteAddress: Address, streamName: String): Unit =
    new TransportRestartOutbound(remoteAddress, streamName).commit()

  override def transportRestartInbound(remoteAddress: UniqueAddress, streamName: String): Unit =
    new TransportRestartInbound(remoteAddress, streamName).commit()

  override def aeronSinkStarted(channel: String, streamId: Int): Unit =
    new AeronSinkStarted(channel, streamId).commit()

  override def aeronSinkTaskRunnerRemoved(channel: String, streamId: Int): Unit =
    new AeronSinkTaskRunnerRemoved(channel, streamId).commit()

  override def aeronSinkPublicationClosed(channel: String, streamId: Int): Unit =
    new AeronSinkPublicationClosed(channel, streamId).commit()

  override def aeronSinkPublicationClosedUnexpectedly(channel: String, streamId: Int): Unit =
    new AeronSinkPublicationClosedUnexpectedly(channel, streamId).commit()

  override def aeronSinkStopped(channel: String, streamId: Int): Unit =
    new AeronSinkStopped(channel, streamId).commit()

  override def aeronSinkEnvelopeGrabbed(lastMessageSize: Int): Unit =
    new AeronSinkEnvelopeGrabbed(lastMessageSize).commit()

  override def aeronSinkEnvelopeOffered(lastMessageSize: Int): Unit =
    new AeronSinkEnvelopeOffered(lastMessageSize).commit()

  override def aeronSinkGaveUpEnvelope(cause: String): Unit =
    new AeronSinkGaveUpEnvelope(cause).commit()

  override def aeronSinkDelegateToTaskRunner(countBeforeDelegate: Long): Unit =
    new AeronSinkDelegateToTaskRunner(countBeforeDelegate).commit()

  override def aeronSinkReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit =
    new AeronSinkReturnFromTaskRunner(nanosSinceTaskStartTime).commit()

  override def aeronSourceStarted(channel: String, streamId: Int): Unit =
    new AeronSourceStarted(channel, streamId).commit()

  override def aeronSourceStopped(channel: String, streamId: Int): Unit =
    new AeronSourceStopped(channel, streamId).commit()

  override def aeronSourceReceived(size: Int): Unit =
    new AeronSourceReceived(size).commit()

  override def aeronSourceDelegateToTaskRunner(countBeforeDelegate: Long): Unit =
    new AeronSourceDelegateToTaskRunner(countBeforeDelegate).commit()

  override def aeronSourceReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit =
    new AeronSourceReturnFromTaskRunner(nanosSinceTaskStartTime).commit()

  override def compressionActorRefAdvertisement(uid: Long): Unit =
    new CompressionActorRefAdvertisement(uid).commit()

  override def compressionClassManifestAdvertisement(uid: Long): Unit =
    new CompressionClassManifestAdvertisement(uid).commit()

  override def tcpOutboundConnected(remoteAddress: Address, streamName: String): Unit =
    new TcpOutboundConnected(remoteAddress, streamName).commit()

  override def tcpOutboundSent(size: Int): Unit =
    new TcpOutboundSent(size).commit()

  override def tcpInboundBound(bindHost: String, address: InetSocketAddress): Unit =
    new TcpInboundBound(bindHost, address).commit()

  override def tcpInboundUnbound(localAddress: UniqueAddress): Unit =
    new TcpInboundUnbound(localAddress).commit()

  override def tcpInboundConnected(remoteAddress: InetSocketAddress): Unit =
    new TcpInboundConnected(remoteAddress).commit()

  override def tcpInboundReceived(size: Int): Unit =
    new TcpInboundReceived(size).commit()
}
