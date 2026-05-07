/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.jfr

import java.net.InetSocketAddress

import jdk.jfr.Category
import jdk.jfr.DataAmount
import jdk.jfr.Enabled
import jdk.jfr.Event
import jdk.jfr.Label
import jdk.jfr.StackTrace
import jdk.jfr.Timespan

import akka.actor.Address
import akka.annotation.InternalApi
import akka.remote.UniqueAddress

// requires jdk9+ to compile
// for editing these in IntelliJ, open module settings, change JDK dependency to 11 for only this module

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object JFREventUtils {

  def stringOf(address: InetSocketAddress): String =
    s"${address.getHostString}:${address.getPort}"

}

// transport events

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron")) @Label("Media driver started")
final class TransportMediaDriverStarted(val directoryName: String) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Transport started")
final class TransportStarted() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron")) @Label("Aeron error log started")
final class TransportAeronErrorLogStarted() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Task runner started")
final class TransportTaskRunnerStarted() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Unique address set")
final class TransportUniqueAddressSet(_uniqueAddress: UniqueAddress) extends Event {
  val uniqueAddress = _uniqueAddress.toString()
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Materializer started")
final class TransportMaterializerStarted() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Startup finished")
final class TransportStartupFinished() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Kill switch pulled")
final class TransportKillSwitchPulled() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Stopped")
final class TransportStopped() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron")) @Label("Aeron log task stopped")
final class TransportAeronErrorLogTaskStopped() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Media file deleted")
final class TransportMediaFileDeleted() extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Send queue overflow")
final class TransportSendQueueOverflow(val queueIndex: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Stop idle outbound")
final class TransportStopIdleOutbound(_remoteAddress: Address, val queueIndex: Int) extends Event {
  val remoteAddress = _remoteAddress.toString
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Quarantined")
final class TransportQuarantined(_remoteAddress: Address, val uid: Long) extends Event {
  val remoteAddress = _remoteAddress.toString
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Remove quarantined")
final class TransportRemoveQuarantined(_remoteAddress: Address) extends Event {
  val remoteAddress = _remoteAddress.toString
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Restart outbound")
final class TransportRestartOutbound(_remoteAddress: Address, val streamName: String) extends Event {
  val remoteAddress = _remoteAddress.toString
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Transport")) @Label("Restart inbound")
final class TransportRestartInbound(_remoteAddress: UniqueAddress, val streamName: String) extends Event {
  val remoteAddress = _remoteAddress.toString()
}

// aeron sink events

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Started")
final class AeronSinkStarted(val channel: String, val streamId: Int) extends Event {}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Task runner removed")
final class AeronSinkTaskRunnerRemoved(val channel: String, val streamId: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Publication closed")
final class AeronSinkPublicationClosed(val channel: String, val streamId: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Publication closed unexpectedly")
final class AeronSinkPublicationClosedUnexpectedly(val channel: String, val streamId: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Stopped")
final class AeronSinkStopped(val channel: String, val streamId: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Envelope grabbed")
final class AeronSinkEnvelopeGrabbed(@DataAmount() val lastMessageSize: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Envelope offered")
final class AeronSinkEnvelopeOffered(@DataAmount() val lastMessageSize: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Gave up envelope")
final class AeronSinkGaveUpEnvelope(val cause: String) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Delegate to task runner")
final class AeronSinkDelegateToTaskRunner(val countBeforeDelegate: Long) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Sink")) @Label("Return from task runner")
final class AeronSinkReturnFromTaskRunner(@Timespan(Timespan.NANOSECONDS) val nanosSinceTaskStartTime: Long)
    extends Event

// aeron source events

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Source")) @Label("Started")
final class AeronSourceStarted(val channel: String, val streamId: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Source")) @Label("Stopped")
final class AeronSourceStopped(val channel: String, val streamId: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Source")) @Label("Received")
final class AeronSourceReceived(@DataAmount() val size: Int) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Source")) @Label("Delegate to task runner")
final class AeronSourceDelegateToTaskRunner(val countBeforeDelegate: Long) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Aeron", "Source")) @Label("Return from task runner")
final class AeronSourceReturnFromTaskRunner(@Timespan(Timespan.NANOSECONDS) val nanosSinceTaskStartTime: Long)
    extends Event

// compression events

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Compression")) @Label("ActorRef advertisement")
final class CompressionActorRefAdvertisement(val uid: Long) extends Event

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Compression")) @Label("ClassManifest advertisement")
final class CompressionClassManifestAdvertisement(val uid: Long) extends Event

// tcp outbound events

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Tcp", "Outbound")) @Label("Connected")
final class TcpOutboundConnected(_remoteAddress: Address, val streamName: String) extends Event {
  val remoteAddress = _remoteAddress.toString
}

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Tcp", "Outbound")) @Label("Sent")
final class TcpOutboundSent(@DataAmount() val size: Int) extends Event

// tcp inbound events

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Tcp", "Inbound")) @Label("Bound")
final class TcpInboundBound(val bindHost: String, _address: InetSocketAddress) extends Event {
  val address = JFREventUtils.stringOf(_address)
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Tcp", "Inbound")) @Label("Unbound")
final class TcpInboundUnbound(_localAddress: UniqueAddress) extends Event {
  val localAddress = _localAddress.toString()
}

/**
 * INTERNAL API
 */
@InternalApi
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Tcp", "Inbound")) @Label("Connected")
final class TcpInboundConnected(_remoteAddress: InetSocketAddress) extends Event {
  val remoteAddress = JFREventUtils.stringOf(_remoteAddress)
}

/**
 * INTERNAL API
 */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Remoting", "Tcp", "Inbound")) @Label("Received")
final class TcpInboundReceived(@DataAmount() val size: Int) extends Event
