/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.DatagramSocket
import java.net.InetSocketAddress

import scala.annotation.nowarn
import scala.collection.immutable

import com.typesafe.config.Config

import akka.actor._
import akka.io.Inet.{ SoJavaFactories, SocketOption }
import akka.util.ByteString
import akka.util.Helpers.Requiring

/**
 * UDP Extension for Akka’s IO layer.
 *
 * This extension implements the connectionless UDP protocol without
 * calling `connect` on the underlying sockets, i.e. without restricting
 * from whom data can be received. For “connected” UDP mode see [[UdpConnected]].
 *
 * For a full description of the design and philosophy behind this IO
 * implementation please refer to <a href="https://akka.io/docs/">the Akka online documentation</a>.
 *
 * The Java API for generating UDP commands is available at [[UdpMessage]].
 */
object Udp extends ExtensionId[UdpExt] with ExtensionIdProvider {

  override def lookup = Udp

  override def createExtension(system: ExtendedActorSystem): UdpExt = new UdpExt(system)

  /**
   * Java API: retrieve the Udp extension for the given system.
   */
  override def get(system: ActorSystem): UdpExt = super.get(system)
  override def get(system: ClassicActorSystemProvider): UdpExt = super.get(system)

  /**
   * The common interface for [[Command]] and [[Event]].
   */
  sealed trait Message

  /**
   * The common type of all commands supported by the UDP implementation.
   */
  trait Command extends SelectionHandler.HasFailureMessage with Message {
    def failureMessage = CommandFailed(this)
  }

  /**
   * Each [[Send]] can optionally request a positive acknowledgment to be sent
   * to the commanding actor. If such notification is not desired the [[Send#ack]]
   * must be set to an instance of this class. The token contained within can be used
   * to recognize which write failed when receiving a [[CommandFailed]] message.
   */
  case class NoAck(token: Any) extends Event

  /**
   * Default [[NoAck]] instance which is used when no acknowledgment information is
   * explicitly provided. Its “token” is `null`.
   */
  object NoAck extends NoAck(null)

  /**
   * This message is understood by the “simple sender” which can be obtained by
   * sending the [[SimpleSender]] query to the [[UdpExt#manager]] as well as by
   * the listener actors which are created in response to [[Bind]]. It will send
   * the given payload data as one UDP datagram to the given target address. The
   * UDP actor will respond with [[CommandFailed]] if the send could not be
   * enqueued to the O/S kernel because the send buffer was full. If the given
   * `ack` is not of type [[NoAck]] the UDP actor will reply with the given
   * object as soon as the datagram has been successfully enqueued to the O/S
   * kernel.
   *
   * The sending UDP socket’s address belongs to the “simple sender” which does
   * not handle inbound datagrams and sends from an ephemeral port; therefore
   * sending using this mechanism is not suitable if replies are expected, use
   * [[Bind]] in that case.
   */
  final case class Send(payload: ByteString, target: InetSocketAddress, ack: Event) extends Command {
    require(ack != null, "ack must be non-null. Use NoAck if you don't want acks.")

    def wantsAck: Boolean = !ack.isInstanceOf[NoAck]
  }
  object Send {
    def apply(data: ByteString, target: InetSocketAddress): Send = Send(data, target, NoAck)
  }

  /**
   * Send this message to the [[UdpExt#manager]] in order to bind to the given
   * local port (or an automatically assigned one if the port number is zero).
   * The listener actor for the newly bound port will reply with a [[Bound]]
   * message, or the manager will reply with a [[CommandFailed]] message.
   */
  @nowarn("msg=deprecated")
  final case class Bind(
      handler: ActorRef,
      localAddress: InetSocketAddress,
      options: immutable.Traversable[SocketOption] = Nil)
      extends Command

  /**
   * Send this message to the listener actor that previously sent a [[Bound]]
   * message in order to close the listening socket. The recipient will reply
   * with an [[Unbound]] message.
   */
  case object Unbind extends Command

  /**
   * Retrieve a reference to a “simple sender” actor of the UDP extension.
   * The newly created “simple sender” will reply with the [[SimpleSenderReady]] notification.
   *
   * The “simple sender” is a convenient service for being able to send datagrams
   * when the originating address is meaningless, i.e. when no reply is expected.
   *
   * The “simple sender” will not stop itself, you will have to send it a [[akka.actor.PoisonPill]]
   * when you want to close the socket.
   */
  @nowarn("msg=deprecated")
  case class SimpleSender(options: immutable.Traversable[SocketOption] = Nil) extends Command
  object SimpleSender extends SimpleSender(Nil)

  /**
   * Send this message to a listener actor (which sent a [[Bound]] message) to
   * have it stop reading datagrams from the network. If the O/S kernel’s receive
   * buffer runs full then subsequent datagrams will be silently discarded.
   * Re-enable reading from the socket using the `ResumeReading` command.
   */
  case object SuspendReading extends Command

  /**
   * This message must be sent to the listener actor to re-enable reading from
   * the socket after a `SuspendReading` command.
   */
  case object ResumeReading extends Command

  /**
   * The common type of all events emitted by the UDP implementation.
   */
  trait Event extends Message

  /**
   * When a listener actor receives a datagram from its socket it will send
   * it to the handler designated in the [[Bind]] message using this message type.
   */
  final case class Received(data: ByteString, sender: InetSocketAddress) extends Event

  /**
   * When a command fails it will be replied to with this message type,
   * wrapping the failing command object.
   */
  final case class CommandFailed(cmd: Command) extends Event

  /**
   * This message is sent by the listener actor in response to a [[Bind]] command.
   * If the address to bind to specified a port number of zero, then this message
   * can be inspected to find out which port was automatically assigned.
   */
  final case class Bound(localAddress: InetSocketAddress) extends Event

  /**
   * The “simple sender” sends this message type in response to a [[SimpleSender]] query.
   */
  sealed trait SimpleSenderReady extends Event
  case object SimpleSenderReady extends SimpleSenderReady

  /**
   * This message is sent by the listener actor in response to an `Unbind` command
   * after the socket has been closed.
   */
  sealed trait Unbound
  case object Unbound extends Unbound

  /**
   * Scala API: This object provides access to all socket options applicable to UDP sockets.
   *
   * For the Java API see [[UdpSO]].
   */
  object SO extends Inet.SoForwarders {

    /**
     * [[akka.io.Inet.SocketOption]] to set the SO_BROADCAST option
     *
     * For more information see [[java.net.DatagramSocket#setBroadcast]]
     */
    final case class Broadcast(on: Boolean) extends SocketOption {
      override def beforeDatagramBind(s: DatagramSocket): Unit = s.setBroadcast(on)
    }

  }

  private[io] class UdpSettings(_config: Config) extends SelectionHandlerSettings(_config) {
    import _config._

    val NrOfSelectors: Int = getInt("nr-of-selectors").requiring(_ > 0, "nr-of-selectors must be > 0")
    val DirectBufferSize: Int = getIntBytes("direct-buffer-size")
    val MaxDirectBufferPoolSize: Int = getInt("direct-buffer-pool-limit")
    val BatchReceiveLimit: Int = getInt("receive-throughput")

    val ManagementDispatcher: String = getString("management-dispatcher")

    override val MaxChannelsPerSelector: Int = if (MaxChannels == -1) -1 else math.max(MaxChannels / NrOfSelectors, 1)

    private[this] def getIntBytes(path: String): Int = {
      val size = getBytes(path)
      require(size < Int.MaxValue, s"$path must be < 2 GiB")
      size.toInt
    }
  }
}

class UdpExt(system: ExtendedActorSystem) extends IO.Extension {

  import Udp.UdpSettings

  val settings: UdpSettings = new UdpSettings(system.settings.config.getConfig("akka.io.udp"))

  val manager: ActorRef = {
    system.systemActorOf(
      props = Props(new UdpManager(this)).withDispatcher(settings.ManagementDispatcher).withDeploy(Deploy.local),
      name = "IO-UDP-FF")
  }

  /**
   * Java API: retrieve the UDP manager actor’s reference.
   */
  def getManager: ActorRef = manager

  /**
   * INTERNAL API
   */
  private[io] val bufferPool: BufferPool =
    new DirectByteBufferPool(settings.DirectBufferSize, settings.MaxDirectBufferPoolSize)
}

/**
 * Java API: factory methods for the message types used when communicating with the Udp service.
 */
object UdpMessage {
  import java.lang.{ Iterable => JIterable }

  import Udp._

  import scala.jdk.CollectionConverters._

  /**
   * Each [[Udp.Send]] can optionally request a positive acknowledgment to be sent
   * to the commanding actor. If such notification is not desired the [[Udp.Send#ack]]
   * must be set to an instance of this class. The token contained within can be used
   * to recognize which write failed when receiving a [[Udp.CommandFailed]] message.
   */
  def noAck(token: AnyRef): NoAck = NoAck(token)

  /**
   * Default [[Udp.NoAck]] instance which is used when no acknowledgment information is
   * explicitly provided. Its “token” is `null`.
   */
  def noAck: NoAck = NoAck

  /**
   * This message is understood by the “simple sender” which can be obtained by
   * sending the [[Udp.SimpleSender]] query to the [[UdpExt#manager]] as well as by
   * the listener actors which are created in response to [[Udp.Bind]]. It will send
   * the given payload data as one UDP datagram to the given target address. The
   * UDP actor will respond with [[Udp.CommandFailed]] if the send could not be
   * enqueued to the O/S kernel because the send buffer was full. If the given
   * `ack` is not of type [[Udp.NoAck]] the UDP actor will reply with the given
   * object as soon as the datagram has been successfully enqueued to the O/S
   * kernel.
   *
   * The sending UDP socket’s address belongs to the “simple sender” which does
   * not handle inbound datagrams and sends from an ephemeral port; therefore
   * sending using this mechanism is not suitable if replies are expected, use
   * [[Udp.Bind]] in that case.
   */
  def send(payload: ByteString, target: InetSocketAddress, ack: Event): Command = Send(payload, target, ack)

  /**
   * The same as `send(payload, target, noAck())`.
   */
  def send(payload: ByteString, target: InetSocketAddress): Command = Send(payload, target)

  /**
   * Send this message to the [[UdpExt#manager]] in order to bind to the given
   * local port (or an automatically assigned one if the port number is zero).
   * The listener actor for the newly bound port will reply with a [[Udp.Bound]]
   * message, or the manager will reply with a [[Udp.CommandFailed]] message.
   */
  def bind(handler: ActorRef, endpoint: InetSocketAddress, options: JIterable[SocketOption]): Command =
    Bind(handler, endpoint, options.asScala.to(immutable.IndexedSeq))

  /**
   * Bind without specifying options.
   */
  def bind(handler: ActorRef, endpoint: InetSocketAddress): Command = Bind(handler, endpoint, Nil)

  /**
   * Send this message to the listener actor that previously sent a [[Udp.Bound]]
   * message in order to close the listening socket. The recipient will reply
   * with an [[Udp.Unbound]] message.
   */
  def unbind: Command = Unbind

  /**
   * Retrieve a reference to a “simple sender” actor of the UDP extension.
   * The newly created “simple sender” will reply with the [[Udp.SimpleSenderReady]] notification.
   *
   * The “simple sender” is a convenient service for being able to send datagrams
   * when the originating address is meaningless, i.e. when no reply is expected.
   *
   * The “simple sender” will not stop itself, you will have to send it a [[akka.actor.PoisonPill]]
   * when you want to close the socket.
   */
  def simpleSender(options: JIterable[SocketOption]): Command = SimpleSender(options.asScala.to(immutable.IndexedSeq))

  /**
   * Retrieve a simple sender without specifying options.
   */
  def simpleSender: Command = SimpleSender

  /**
   * Send this message to a listener actor (which sent a [[Udp.Bound]] message) to
   * have it stop reading datagrams from the network. If the O/S kernel’s receive
   * buffer runs full then subsequent datagrams will be silently discarded.
   * Re-enable reading from the socket using the `Udp.ResumeReading` command.
   */
  def suspendReading: Command = SuspendReading

  /**
   * This message must be sent to the listener actor to re-enable reading from
   * the socket after a `Udp.SuspendReading` command.
   */
  def resumeReading: Command = ResumeReading
}

object UdpSO extends SoJavaFactories {
  import Udp.SO._

  /**
   * [[akka.io.Inet.SocketOption]] to set the SO_BROADCAST option
   *
   * For more information see [[java.net.DatagramSocket#setBroadcast]]
   */
  def broadcast(on: Boolean) = Broadcast(on)
}
