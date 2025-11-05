/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.net.InetSocketAddress

import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalApi
import akka.remote.UniqueAddress
import akka.util.FlightRecorderLoader

/**
 * INTERNAL API
 */
@InternalApi
object RemotingFlightRecorder extends ExtensionId[RemotingFlightRecorder] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): RemotingFlightRecorder =
    FlightRecorderLoader.load[RemotingFlightRecorder](
      system,
      "akka.remote.artery.jfr.JFRRemotingFlightRecorder",
      NoOpRemotingFlightRecorder)

  override def lookup: ExtensionId[_ <: Extension] = this
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait RemotingFlightRecorder extends Extension {

  def transportMediaDriverStarted(directoryName: String): Unit
  def transportStarted(): Unit
  def transportAeronErrorLogStarted(): Unit
  def transportTaskRunnerStarted(): Unit
  def transportUniqueAddressSet(uniqueAddress: UniqueAddress): Unit
  def transportMaterializerStarted(): Unit
  def transportStartupFinished(): Unit
  def transportKillSwitchPulled(): Unit
  def transportStopped(): Unit
  def transportAeronErrorLogTaskStopped(): Unit
  def transportMediaFileDeleted(): Unit
  def transportSendQueueOverflow(queueIndex: Int): Unit
  def transportStopIdleOutbound(remoteAddress: Address, queueIndex: Int): Unit
  def transportQuarantined(remoteAddress: Address, uid: Long): Unit
  def transportRemoveQuarantined(remoteAddress: Address): Unit
  def transportRestartOutbound(remoteAddress: Address, streamName: String): Unit
  def transportRestartInbound(remoteAddress: UniqueAddress, streamName: String): Unit

  def aeronSinkStarted(channel: String, streamId: Int): Unit
  def aeronSinkTaskRunnerRemoved(channel: String, streamId: Int): Unit
  def aeronSinkPublicationClosed(channel: String, streamId: Int): Unit
  def aeronSinkPublicationClosedUnexpectedly(channel: String, streamId: Int): Unit
  def aeronSinkStopped(channel: String, streamId: Int): Unit
  def aeronSinkEnvelopeGrabbed(lastMessageSize: Int): Unit
  def aeronSinkEnvelopeOffered(lastMessageSize: Int): Unit
  def aeronSinkGaveUpEnvelope(cause: String): Unit
  def aeronSinkDelegateToTaskRunner(countBeforeDelegate: Long): Unit
  def aeronSinkReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit

  def aeronSourceStarted(channel: String, streamId: Int): Unit
  def aeronSourceStopped(channel: String, streamId: Int): Unit
  def aeronSourceReceived(size: Int): Unit
  def aeronSourceDelegateToTaskRunner(countBeforeDelegate: Long): Unit
  def aeronSourceReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit

  def compressionActorRefAdvertisement(uid: Long): Unit
  def compressionClassManifestAdvertisement(uid: Long): Unit

  def tcpOutboundConnected(remoteAddress: Address, streamName: String): Unit
  def tcpOutboundSent(size: Int): Unit

  def tcpInboundBound(bindHost: String, address: InetSocketAddress): Unit
  def tcpInboundUnbound(localAddress: UniqueAddress): Unit
  def tcpInboundConnected(remoteAddress: InetSocketAddress): Unit
  def tcpInboundReceived(size: Int): Unit

}

/**
 * JFR is only available under certain circumstances (JDK11 for now, possible OpenJDK 8 in the future) so therefore
 * the default on JDK 8 needs to be a no-op flight recorder.
 *
 * INTERNAL
 */
@InternalApi
private[akka] case object NoOpRemotingFlightRecorder extends RemotingFlightRecorder {
  override def transportMediaDriverStarted(directoryName: String): Unit = ()
  override def transportStarted(): Unit = ()
  override def transportAeronErrorLogStarted(): Unit = ()
  override def transportTaskRunnerStarted(): Unit = ()
  override def transportUniqueAddressSet(uniqueAddress: UniqueAddress): Unit = ()
  override def transportMaterializerStarted(): Unit = ()
  override def transportStartupFinished(): Unit = ()
  override def transportKillSwitchPulled(): Unit = ()
  override def transportStopped(): Unit = ()
  override def transportAeronErrorLogTaskStopped(): Unit = ()
  override def transportMediaFileDeleted(): Unit = ()
  override def transportStopIdleOutbound(remoteAddress: Address, queueIndex: Int): Unit = ()
  override def transportQuarantined(remoteAddress: Address, uid: Long): Unit = ()
  override def transportRemoveQuarantined(remoteAddress: Address): Unit = ()
  override def transportRestartOutbound(remoteAddress: Address, streamName: String): Unit = ()
  override def transportRestartInbound(remoteAddress: UniqueAddress, streamName: String): Unit = ()
  override def transportSendQueueOverflow(queueIndex: Int): Unit = ()

  override def aeronSinkStarted(channel: String, streamId: Int): Unit = ()
  override def aeronSinkTaskRunnerRemoved(channel: String, streamId: Int): Unit = ()
  override def aeronSinkPublicationClosed(channel: String, streamId: Int): Unit = ()
  override def aeronSinkPublicationClosedUnexpectedly(channel: String, streamId: Int): Unit = ()
  override def aeronSinkStopped(channel: String, streamId: Int): Unit = ()
  override def aeronSinkEnvelopeGrabbed(lastMessageSize: Int): Unit = ()
  override def aeronSinkEnvelopeOffered(lastMessageSize: Int): Unit = ()
  override def aeronSinkGaveUpEnvelope(cause: String): Unit = ()
  override def aeronSinkDelegateToTaskRunner(countBeforeDelegate: Long): Unit = ()
  override def aeronSinkReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit = ()

  override def aeronSourceStarted(channel: String, streamId: Int): Unit = ()
  override def aeronSourceStopped(channel: String, streamId: Int): Unit = ()
  override def aeronSourceReceived(size: Int): Unit = ()
  override def aeronSourceDelegateToTaskRunner(countBeforeDelegate: Long): Unit = ()
  override def aeronSourceReturnFromTaskRunner(nanosSinceTaskStartTime: Long): Unit = ()

  override def compressionActorRefAdvertisement(uid: Long): Unit = ()
  override def compressionClassManifestAdvertisement(uid: Long): Unit = ()
  override def tcpOutboundConnected(remoteAddress: Address, streamName: String): Unit = ()
  override def tcpOutboundSent(size: Int): Unit = ()
  override def tcpInboundBound(bindHost: String, address: InetSocketAddress): Unit = ()
  override def tcpInboundUnbound(localAddress: UniqueAddress): Unit = ()
  override def tcpInboundConnected(remoteAddress: InetSocketAddress): Unit = ()
  override def tcpInboundReceived(size: Int): Unit = ()

}
