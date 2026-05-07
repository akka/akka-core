/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.lang.{ Iterable => JIterable }
import java.net.InetSocketAddress

import scala.annotation.nowarn
import scala.collection.immutable

import akka.actor._
import akka.io.Inet.SocketOption
import akka.io.Udp.UdpSettings
import akka.util.ByteString

/**
 * UDP Extension for Akka’s IO layer.
 *
 * This extension implements the connectionless UDP protocol with
 * calling `connect` on the underlying sockets, i.e. with restricting
 * from whom data can be received. For “unconnected” UDP mode see [[Udp]].
 *
 * For a full description of the design and philosophy behind this IO
 * implementation please refer to <a href="https://akka.io/docs/">the Akka online documentation</a>.
 *
 * The Java API for generating UDP commands is available at [[UdpConnectedMessage]].
 */
object UdpConnected extends ExtensionId[UdpConnectedExt] with ExtensionIdProvider {

  override def lookup = UdpConnected

  override def createExtension(system: ExtendedActorSystem): UdpConnectedExt = new UdpConnectedExt(system)

  /**
   * Java API: retrieve the UdpConnected extension for the given system.
   */
  override def get(system: ActorSystem): UdpConnectedExt = super.get(system)
  override def get(system: ClassicActorSystemProvider): UdpConnectedExt = super.get(system)

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
   * This message is understood by the connection actors to send data to their
   * designated destination. The connection actor will respond with
   * [[CommandFailed]] if the send could not be enqueued to the O/S kernel
   * because the send buffer was full. If the given `ack` is not of type [[NoAck]]
   * the connection actor will reply with the given object as soon as the datagram
   * has been successfully enqueued to the O/S kernel.
   */
  final case class Send(payload: ByteString, ack: Any) extends Command {
    require(
      ack
        != null,
      "ack must be non-null. Use NoAck if you don't want acks.")

    def wantsAck: Boolean = !ack.isInstanceOf[NoAck]
  }
  object Send {
    def apply(data: ByteString): Send = Send(data, NoAck)
  }

  /**
   * Send this message to the [[UdpExt#manager]] in order to bind to a local
   * port (optionally with the chosen `localAddress`) and create a UDP socket
   * which is restricted to sending to and receiving from the given `remoteAddress`.
   * All received datagrams will be sent to the designated `handler` actor.
   */
  @nowarn("msg=deprecated")
  final case class Connect(
      handler: ActorRef,
      remoteAddress: InetSocketAddress,
      localAddress: Option[InetSocketAddress] = None,
      options: immutable.Traversable[SocketOption] = Nil)
      extends Command

  /**
   * Send this message to a connection actor (which had previously sent the
   * [[Connected]] message) in order to close the socket. The connection actor
   * will reply with a [[Disconnected]] message.
   */
  case object Disconnect extends Command

  /**
   * Send this message to a listener actor (which sent a [[Udp.Bound]] message) to
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
   * When a connection actor receives a datagram from its socket it will send
   * it to the handler designated in the [[Udp.Bind]] message using this message type.
   */
  final case class Received(data: ByteString) extends Event

  /**
   * When a command fails it will be replied to with this message type,
   * wrapping the failing command object.
   */
  final case class CommandFailed(cmd: Command) extends Event

  /**
   * This message is sent by the connection actor to the actor which sent the
   * [[Connect]] message when the UDP socket has been bound to the local and
   * remote addresses given.
   */
  sealed trait Connected extends Event
  case object Connected extends Connected

  /**
   * This message is sent by the connection actor to the actor which sent the
   * `Disconnect` message when the UDP socket has been closed.
   */
  sealed trait Disconnected extends Event
  case object Disconnected extends Disconnected

}

class UdpConnectedExt(system: ExtendedActorSystem) extends IO.Extension {

  val settings: UdpSettings = new UdpSettings(system.settings.config.getConfig("akka.io.udp-connected"))

  val manager: ActorRef = {
    system.systemActorOf(
      props =
        Props(new UdpConnectedManager(this)).withDispatcher(settings.ManagementDispatcher).withDeploy(Deploy.local),
      name = "IO-UDP-CONN")
  }

  /**
   * Java API: retrieve the UDP manager actor’s reference.
   */
  def getManager: ActorRef = manager

  val bufferPool: BufferPool = new DirectByteBufferPool(settings.DirectBufferSize, settings.MaxDirectBufferPoolSize)

}

/**
 * Java API: factory methods for the message types used when communicating with the UdpConnected service.
 */
object UdpConnectedMessage {
  import UdpConnected._
  import language.implicitConversions

  /**
   * Send this message to the [[UdpExt#manager]] in order to bind to a local
   * port (optionally with the chosen `localAddress`) and create a UDP socket
   * which is restricted to sending to and receiving from the given `remoteAddress`.
   * All received datagrams will be sent to the designated `handler` actor.
   */
  def connect(
      handler: ActorRef,
      remoteAddress: InetSocketAddress,
      localAddress: InetSocketAddress,
      options: JIterable[SocketOption]): Command = Connect(handler, remoteAddress, Some(localAddress), options)

  /**
   * Connect without specifying the `localAddress`.
   */
  def connect(handler: ActorRef, remoteAddress: InetSocketAddress, options: JIterable[SocketOption]): Command =
    Connect(handler, remoteAddress, None, options)

  /**
   * Connect without specifying the `localAddress` or `options`.
   */
  def connect(handler: ActorRef, remoteAddress: InetSocketAddress): Command = Connect(handler, remoteAddress, None, Nil)

  /**
   * This message is understood by the connection actors to send data to their
   * designated destination. The connection actor will respond with
   * [[UdpConnected.CommandFailed]] if the send could not be enqueued to the O/S kernel
   * because the send buffer was full. If the given `ack` is not of type [[UdpConnected.NoAck]]
   * the connection actor will reply with the given object as soon as the datagram
   * has been successfully enqueued to the O/S kernel.
   */
  def send(data: ByteString, ack: AnyRef): Command = Send(data, ack)

  /**
   * Send without requesting acknowledgment.
   */
  def send(data: ByteString): Command = Send(data)

  /**
   * Send this message to a connection actor (which had previously sent the
   * [[UdpConnected.Connected]] message) in order to close the socket. The connection actor
   * will reply with a [[UdpConnected.Disconnected]] message.
   */
  def disconnect: Command = Disconnect

  /**
   * Each [[UdpConnected.Send]] can optionally request a positive acknowledgment to be sent
   * to the commanding actor. If such notification is not desired the [[UdpConnected.Send#ack]]
   * must be set to an instance of this class. The token contained within can be used
   * to recognize which write failed when receiving a [[UdpConnected.CommandFailed]] message.
   */
  def noAck(token: AnyRef): NoAck = NoAck(token)

  /**
   * Default [[UdpConnected.NoAck]] instance which is used when no acknowledgment information is
   * explicitly provided. Its “token” is `null`.
   */
  def noAck: NoAck = NoAck

  /**
   * Send this message to a listener actor (which sent a [[Udp.Bound]] message) to
   * have it stop reading datagrams from the network. If the O/S kernel’s receive
   * buffer runs full then subsequent datagrams will be silently discarded.
   * Re-enable reading from the socket using the `UdpConnected.ResumeReading` command.
   */
  def suspendReading: Command = SuspendReading

  /**
   * This message must be sent to the listener actor to re-enable reading from
   * the socket after a `UdpConnected.SuspendReading` command.
   */
  def resumeReading: Command = ResumeReading

  implicit private def fromJava[T](coll: JIterable[T]): immutable.Iterable[T] = {
    import scala.jdk.CollectionConverters._
    coll.asScala.to(immutable.Iterable)
  }
}
