/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.net.InetSocketAddress

import akka.actor.Address
import akka.annotation.InternalApi
import akka.remote.UniqueAddress
import akka.remote.artery.jfr.AeronSinkDelegateToTaskRunner
import akka.remote.artery.jfr.AeronSinkEnvelopeGrabbed
import akka.remote.artery.jfr.AeronSinkEnvelopeOffered
import akka.remote.artery.jfr.AeronSinkGaveUpEnvelope
import akka.remote.artery.jfr.AeronSinkPublicationClosed
import akka.remote.artery.jfr.AeronSinkPublicationClosedUnexpectedly
import akka.remote.artery.jfr.AeronSinkReturnFromTaskRunner
import akka.remote.artery.jfr.AeronSinkStarted
import akka.remote.artery.jfr.AeronSinkStopped
import akka.remote.artery.jfr.AeronSinkTaskRunnerRemoved
import akka.remote.artery.jfr.AeronSourceDelegateToTaskRunner
import akka.remote.artery.jfr.AeronSourceReceived
import akka.remote.artery.jfr.AeronSourceReturnFromTaskRunner
import akka.remote.artery.jfr.AeronSourceStarted
import akka.remote.artery.jfr.AeronSourceStopped
import akka.remote.artery.jfr.CompressionActorRefAdvertisement
import akka.remote.artery.jfr.CompressionClassManifestAdvertisement
import akka.remote.artery.jfr.TcpInboundBound
import akka.remote.artery.jfr.TcpInboundConnected
import akka.remote.artery.jfr.TcpInboundReceived
import akka.remote.artery.jfr.TcpInboundUnbound
import akka.remote.artery.jfr.TcpOutboundConnected
import akka.remote.artery.jfr.TcpOutboundSent
import akka.remote.artery.jfr.TransportAeronErrorLogStarted
import akka.remote.artery.jfr.TransportAeronErrorLogTaskStopped
import akka.remote.artery.jfr.TransportKillSwitchPulled
import akka.remote.artery.jfr.TransportMaterializerStarted
import akka.remote.artery.jfr.TransportMediaDriverStarted
import akka.remote.artery.jfr.TransportMediaFileDeleted
import akka.remote.artery.jfr.TransportQuarantined
import akka.remote.artery.jfr.TransportRemoveQuarantined
import akka.remote.artery.jfr.TransportRestartInbound
import akka.remote.artery.jfr.TransportRestartOutbound
import akka.remote.artery.jfr.TransportSendQueueOverflow
import akka.remote.artery.jfr.TransportStarted
import akka.remote.artery.jfr.TransportStartupFinished
import akka.remote.artery.jfr.TransportStopIdleOutbound
import akka.remote.artery.jfr.TransportStopped
import akka.remote.artery.jfr.TransportTaskRunnerStarted
import akka.remote.artery.jfr.TransportUniqueAddressSet

/**
 * INTERNAL API
 */
@InternalApi
object RemotingFlightRecorder {
  def transportMediaDriverStarted(directoryName: String): Unit =
    new TransportMediaDriverStarted(directoryName).commit()

  def transportStarted(): Unit =
    new TransportStarted().commit()

  def transportAeronErrorLogStarted(): Unit =
    new TransportAeronErrorLogStarted().commit()

  def transportTaskRunnerStarted(): Unit =
    new TransportTaskRunnerStarted().commit()

  def transportUniqueAddressSet(uniqueAddress: UniqueAddress): Unit =
    new TransportUniqueAddressSet(uniqueAddress).commit()

  def transportMaterializerStarted(): Unit =
    new TransportMaterializerStarted().commit()

  def transportStartupFinished(): Unit =
    new TransportStartupFinished().commit()

  def transportKillSwitchPulled(): Unit =
    new TransportKillSwitchPulled().commit()

  def transportStopped(): Unit =
    new TransportStopped().commit()

  def transportAeronErrorLogTaskStopped(): Unit =
    new TransportAeronErrorLogTaskStopped().commit()

  def transportMediaFileDeleted(): Unit =
    new TransportMediaFileDeleted().commit()

  def transportSendQueueOverflow(queueIndex: Int): Unit =
    new TransportSendQueueOverflow(queueIndex).commit()

  def transportStopIdleOutbound(remoteAddress: Address, queueIndex: Int): Unit =
    new TransportStopIdleOutbound(remoteAddress, queueIndex).commit()

  def transportQuarantined(remoteAddress: Address, uid: Long): Unit =
    new TransportQuarantined(remoteAddress, uid).commit()

  def transportRemoveQuarantined(remoteAddress: Address): Unit =
    new TransportRemoveQuarantined(remoteAddress).commit()

  def transportRestartOutbound(remoteAddress: Address, streamName: String): Unit =
    new TransportRestartOutbound(remoteAddress, streamName).commit()

  def transportRestartInbound(remoteAddress: UniqueAddress, streamName: String): Unit =
    new TransportRestartInbound(remoteAddress, streamName).commit()

  def aeronSinkStarted(channel: String, streamId: Int): Unit =
    new AeronSinkStarted(channel, streamId).commit()

  def aeronSinkTaskRunnerRemoved(channel: String, streamId: Int): Unit =
    new AeronSinkTaskRunnerRemoved(channel, streamId).commit()

  def aeronSinkPublicationClosed(channel: String, streamId: Int): Unit =
    new AeronSinkPublicationClosed(channel, streamId).commit()

  def aeronSinkPublicationClosedUnexpectedly(channel: String, streamId: Int): Unit =
    new AeronSinkPublicationClosedUnexpectedly(channel, streamId).commit()

  def aeronSinkStopped(channel: String, streamId: Int): Unit =
    new AeronSinkStopped(channel, streamId).commit()

  def aeronSinkEnvelopeGrabbed(lastMessageSize: Int): Unit =
    new AeronSinkEnvelopeGrabbed(lastMessageSize).commit()

  def aeronSinkEnvelopeOffered(lastMessageSize: Int): Unit =
    new AeronSinkEnvelopeOffered(lastMessageSize).commit()

  def aeronSinkGaveUpEnvelope(cause: String): Unit =
    new AeronSinkGaveUpEnvelope(cause).commit()

  def aeronSinkDelegateToTaskRunner(countBeforeDelegate: Long): Unit =
    new AeronSinkDelegateToTaskRunner(countBeforeDelegate).commit()

  def aeronSinkReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit =
    new AeronSinkReturnFromTaskRunner(nanosSinceTaskStartTime).commit()

  def aeronSourceStarted(channel: String, streamId: Int): Unit =
    new AeronSourceStarted(channel, streamId).commit()

  def aeronSourceStopped(channel: String, streamId: Int): Unit =
    new AeronSourceStopped(channel, streamId).commit()

  def aeronSourceReceived(size: Int): Unit =
    new AeronSourceReceived(size).commit()

  def aeronSourceDelegateToTaskRunner(countBeforeDelegate: Long): Unit =
    new AeronSourceDelegateToTaskRunner(countBeforeDelegate).commit()

  def aeronSourceReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit =
    new AeronSourceReturnFromTaskRunner(nanosSinceTaskStartTime).commit()

  def compressionActorRefAdvertisement(uid: Long): Unit =
    new CompressionActorRefAdvertisement(uid).commit()

  def compressionClassManifestAdvertisement(uid: Long): Unit =
    new CompressionClassManifestAdvertisement(uid).commit()

  def tcpOutboundConnected(remoteAddress: Address, streamName: String): Unit =
    new TcpOutboundConnected(remoteAddress, streamName).commit()

  def tcpOutboundSent(size: Int): Unit =
    new TcpOutboundSent(size).commit()

  def tcpInboundBound(bindHost: String, address: InetSocketAddress): Unit =
    new TcpInboundBound(bindHost, address).commit()

  def tcpInboundUnbound(localAddress: UniqueAddress): Unit =
    new TcpInboundUnbound(localAddress).commit()

  def tcpInboundConnected(remoteAddress: InetSocketAddress): Unit =
    new TcpInboundConnected(remoteAddress).commit()

  def tcpInboundReceived(size: Int): Unit =
    new TcpInboundReceived(size).commit()
}
