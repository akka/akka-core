/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.lang.{ Iterable => JIterable }
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.file.{ Path, Paths }

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor._
import akka.annotation.InternalApi
import akka.io.Inet._
import akka.util.{ ByteString, Helpers }
import akka.util.Helpers.Requiring
import scala.jdk.CollectionConverters._

/**
 * TCP Extension for Akka’s IO layer.
 *
 * For a full description of the design and philosophy behind this IO
 * implementation please refer to <a href="https://akka.io/docs/">the Akka online documentation</a>.
 *
 * In order to open an outbound connection send a [[Tcp.Connect]] message
 * to the [[TcpExt#manager]].
 *
 * In order to start listening for inbound connections send a [[Tcp.Bind]]
 * message to the [[TcpExt#manager]].
 *
 * The Java API for generating TCP commands is available at [[TcpMessage]].
 */
object Tcp extends ExtensionId[TcpExt] with ExtensionIdProvider {

  override def lookup = Tcp

  override def createExtension(system: ExtendedActorSystem): TcpExt = new TcpExt(system)

  /**
   * Java API: retrieve the Tcp extension for the given system.
   */
  override def get(system: ActorSystem): TcpExt = super.get(system)

  override def get(system: ClassicActorSystemProvider): TcpExt = super.get(system)

  /**
   * Scala API: this object contains all applicable socket options for TCP.
   *
   * For the Java API see [[TcpSO]].
   */
  object SO extends Inet.SoForwarders {

    // general socket options

    /**
     * [[akka.io.Inet.SocketOption]] to enable or disable SO_KEEPALIVE
     *
     * For more information see `java.net.Socket.setKeepAlive`
     */
    final case class KeepAlive(on: Boolean) extends SocketOption {
      override def afterConnect(s: Socket): Unit = s.setKeepAlive(on)
    }

    /**
     * [[akka.io.Inet.SocketOption]] to enable or disable OOBINLINE (receipt
     * of TCP urgent data) By default, this option is disabled and TCP urgent
     * data is silently discarded.
     *
     * For more information see `java.net.Socket.setOOBInline`
     */
    final case class OOBInline(on: Boolean) extends SocketOption {
      override def afterConnect(s: Socket): Unit = s.setOOBInline(on)
    }

    // SO_LINGER is handled by the Close code

    /**
     * [[akka.io.Inet.SocketOption]] to enable or disable TCP_NODELAY
     * (disable or enable Nagle's algorithm)
     *
     * Please note, that TCP_NODELAY is enabled by default.
     *
     * For more information see `java.net.Socket.setTcpNoDelay`
     */
    final case class TcpNoDelay(on: Boolean) extends SocketOption {
      override def afterConnect(s: Socket): Unit = s.setTcpNoDelay(on)
    }

  }

  /**
   * The common interface for [[Command]] and [[Event]].
   */
  sealed trait Message extends NoSerializationVerificationNeeded

  /// COMMANDS

  /**
   * This is the common trait for all commands understood by TCP actors.
   */
  trait Command extends Message with SelectionHandler.HasFailureMessage {
    def failureMessage = CommandFailed(this)
  }

  /**
   * The Connect message is sent to the TCP manager actor, which is obtained via
   * [[TcpExt#manager]]. Either the manager replies with a [[CommandFailed]]
   * or the actor handling the new connection replies with a [[Connected]]
   * message.
   *
   * @param remoteAddress is the address to connect to
   * @param localAddress optionally specifies a specific address to bind to
   * @param options Please refer to the `Tcp.SO` object for a list of all supported options.
   */
  @nowarn("msg=deprecated")
  final case class Connect(
      remoteAddress: InetSocketAddress,
      localAddress: Option[InetSocketAddress] = None,
      options: immutable.Traversable[SocketOption] = Nil,
      timeout: Option[FiniteDuration] = None,
      pullMode: Boolean = false)
      extends Command

  /**
   * The Bind message is send to the TCP manager actor, which is obtained via
   * [[TcpExt#manager]] in order to bind to a listening socket. The manager
   * replies either with a [[CommandFailed]] or the actor handling the listen
   * socket replies with a [[Bound]] message. If the local port is set to 0 in
   * the Bind message, then the [[Bound]] message should be inspected to find
   * the actual port which was bound to.
   *
   * @param handler The actor which will receive all incoming connection requests
   *                in the form of [[Connected]] messages.
   *
   * @param localAddress The socket address to bind to; use port zero for
   *                automatic assignment (i.e. an ephemeral port, see [[Bound]])
   *
   * @param backlog This specifies the number of unaccepted connections the O/S
   *                kernel will hold for this port before refusing connections.
   *
   * @param options Please refer to the `Tcp.SO` object for a list of all supported options.
   */
  @nowarn("msg=deprecated")
  final case class Bind(
      handler: ActorRef,
      localAddress: InetSocketAddress,
      backlog: Int = 100,
      options: immutable.Traversable[SocketOption] = Nil,
      pullMode: Boolean = false)
      extends Command

  /**
   * This message must be sent to a TCP connection actor after receiving the
   * [[Connected]] message. The connection will not read any data from the
   * socket until this message is received, because this message defines the
   * actor which will receive all inbound data.
   *
   * @param handler The actor which will receive all incoming data and which
   *                will be informed when the connection is closed.
   *
   * @param keepOpenOnPeerClosed If this is set to true then the connection
   *                is not automatically closed when the peer closes its half,
   *                requiring an explicit [[CloseCommand]] from our side when finished.
   *
   * @param useResumeWriting If this is set to true then the connection actor
   *                will refuse all further writes after issuing a [[CommandFailed]]
   *                notification until `ResumeWriting` is received. This can
   *                be used to implement NACK-based write backpressure.
   */
  final case class Register(handler: ActorRef, keepOpenOnPeerClosed: Boolean = false, useResumeWriting: Boolean = true)
      extends Command

  /**
   * In order to close down a listening socket, send this message to that socket’s
   * actor (that is the actor which previously had sent the [[Bound]] message). The
   * listener socket actor will reply with a [[Unbound]] message.
   */
  case object Unbind extends Command

  /**
   * Common interface for all commands which aim to close down an open connection.
   */
  sealed trait CloseCommand extends Command with DeadLetterSuppression {

    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    def event: ConnectionClosed
  }

  /**
   * A normal close operation will first flush pending writes and then close the
   * socket. The sender of this command and the registered handler for incoming
   * data will both be notified once the socket is closed using a `Closed`
   * message.
   */
  case object Close extends CloseCommand {

    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    override def event = Closed
  }

  /**
   * A confirmed close operation will flush pending writes and half-close the
   * connection, waiting for the peer to close the other half. The sender of this
   * command and the registered handler for incoming data will both be notified
   * once the socket is closed using a `ConfirmedClosed` message.
   */
  case object ConfirmedClose extends CloseCommand {

    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    override def event = ConfirmedClosed
  }

  /**
   * An abort operation will not flush pending writes and will issue a TCP ABORT
   * command to the O/S kernel which should result in a TCP_RST packet being sent
   * to the peer. The sender of this command and the registered handler for
   * incoming data will both be notified once the socket is closed using a
   * `Aborted` message.
   */
  case object Abort extends CloseCommand {

    /**
     * The corresponding event which is sent as an acknowledgment once the
     * close operation is finished.
     */
    override def event = Aborted
  }

  /**
   * Each [[WriteCommand]] can optionally request a positive acknowledgment to be sent
   * to the commanding actor. If such notification is not desired the [[SimpleWriteCommand#ack]]
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
   * Common interface for all write commands.
   */
  sealed abstract class WriteCommand extends Command {

    /**
     * Prepends this command with another `Write` or `WriteFile` to form
     * a `CompoundWrite`.
     */
    def +:(other: SimpleWriteCommand): CompoundWrite = CompoundWrite(other, this)

    /**
     * Prepends this command with a number of other writes.
     * The first element of the given Iterable becomes the first sub write of a potentially
     * created `CompoundWrite`.
     */
    def ++:(writes: Iterable[WriteCommand]): WriteCommand =
      writes.foldRight(this) {
        case (a: SimpleWriteCommand, b) => a +: b
        case (a: CompoundWrite, b)      => a ++: b
      }

    /**
     * Java API: prepends this command with another `Write` or `WriteFile` to form
     * a `CompoundWrite`.
     */
    def prepend(that: SimpleWriteCommand): CompoundWrite = that +: this

    /**
     * Java API: prepends this command with a number of other writes.
     * The first element of the given Iterable becomes the first sub write of a potentially
     * created `CompoundWrite`.
     */
    def prepend(writes: JIterable[WriteCommand]): WriteCommand = writes.asScala ++: this
  }

  object WriteCommand {

    /**
     * Combines the given number of write commands into one atomic `WriteCommand`.
     */
    def apply(writes: Iterable[WriteCommand]): WriteCommand = writes ++: Write.empty

    /**
     * Java API: combines the given number of write commands into one atomic `WriteCommand`.
     */
    def create(writes: JIterable[WriteCommand]): WriteCommand = apply(writes.asScala)
  }

  /**
   * Common supertype of [[Write]] and [[WriteFile]].
   */
  sealed abstract class SimpleWriteCommand extends WriteCommand {
    require(ack != null, "ack must be non-null. Use NoAck if you don't want acks.")

    /**
     * The acknowledgment token associated with this write command.
     */
    def ack: Event

    /**
     * An acknowledgment is only sent if this write command “wants an ack”, which is
     * equivalent to the [[#ack]] token not being a of type [[NoAck]].
     */
    def wantsAck: Boolean = !ack.isInstanceOf[NoAck]

    /**
     * Java API: appends this command with another `WriteCommand` to form a `CompoundWrite`.
     */
    def append(that: WriteCommand): CompoundWrite = this +: that
  }

  /**
   * Write data to the TCP connection. If no ack is needed use the special
   * `NoAck` object. The connection actor will reply with a [[CommandFailed]]
   * message if the write could not be enqueued. If [[SimpleWriteCommand#wantsAck]]
   * returns true, the connection actor will reply with the supplied [[SimpleWriteCommand#ack]]
   * token once the write has been successfully enqueued to the O/S kernel.
   * <b>Note that this does not in any way guarantee that the data will be
   * or have been sent!</b> Unfortunately there is no way to determine whether
   * a particular write has been sent by the O/S.
   */
  final case class Write(data: ByteString, ack: Event) extends SimpleWriteCommand
  object Write {

    /**
     * The empty Write doesn't write anything and isn't acknowledged.
     * It will, however, be denied and sent back with `CommandFailed` if the
     * connection isn't currently ready to send any data (because another WriteCommand
     * is still pending).
     */
    val empty: Write = Write(ByteString.empty, NoAck)

    /**
     * Create a new unacknowledged Write command with the given data.
     */
    def apply(data: ByteString): Write =
      if (data.isEmpty) empty else Write(data, NoAck)
  }

  /**
   * @see [[WritePath]]
   */
  @deprecated("Use WritePath instead", "2.5.10")
  final case class WriteFile(filePath: String, position: Long, count: Long, ack: Event) extends SimpleWriteCommand {
    require(position >= 0, "WriteFile.position must be >= 0")
    require(count > 0, "WriteFile.count must be > 0")
  }

  /**
   * Write `count` bytes starting at `position` from file at `filePath` to the connection.
   * The count must be &gt; 0. The connection actor will reply with a [[CommandFailed]]
   * message if the write could not be enqueued. If [[SimpleWriteCommand#wantsAck]]
   * returns true, the connection actor will reply with the supplied [[SimpleWriteCommand#ack]]
   * token once the write has been successfully enqueued to the O/S kernel.
   * <b>Note that this does not in any way guarantee that the data will be
   * or have been sent!</b> Unfortunately there is no way to determine whether
   * a particular write has been sent by the O/S.
   */
  final case class WritePath(path: Path, position: Long, count: Long, ack: Event) extends SimpleWriteCommand {
    require(position >= 0, "WriteFile.position must be >= 0")
    require(count > 0, "WriteFile.count must be > 0")
  }

  /**
   * A write command which aggregates two other write commands. Using this construct
   * you can chain a number of [[Write]] and/or [[WriteFile]] commands together in a way
   * that allows them to be handled as a single write which gets written out to the
   * network as quickly as possible.
   * If the sub commands contain `ack` requests they will be honored as soon as the
   * respective write has been written completely.
   */
  final case class CompoundWrite(override val head: SimpleWriteCommand, tailCommand: WriteCommand)
      extends WriteCommand
      with immutable.Iterable[SimpleWriteCommand] {

    def iterator: Iterator[SimpleWriteCommand] =
      new Iterator[SimpleWriteCommand] {
        private[this] var current: WriteCommand = CompoundWrite.this
        def hasNext: Boolean = current ne null
        def next(): SimpleWriteCommand =
          current match {
            case null                  => Iterator.empty.next()
            case CompoundWrite(h, t)   => { current = t; h }
            case x: SimpleWriteCommand => { current = null; x }
          }
      }
  }

  /**
   * When `useResumeWriting` is in effect as was indicated in the [[Register]] message
   * then this command needs to be sent to the connection actor in order to re-enable
   * writing after a [[CommandFailed]] event. All [[WriteCommand]] processed by the
   * connection actor between the first [[CommandFailed]] and subsequent reception of
   * this message will also be rejected with [[CommandFailed]].
   */
  case object ResumeWriting extends Command

  /**
   * Sending this command to the connection actor will disable reading from the TCP
   * socket. TCP flow-control will then propagate backpressure to the sender side
   * as buffers fill up on either end. To re-enable reading send `ResumeReading`.
   */
  case object SuspendReading extends Command

  /**
   * This command needs to be sent to the connection actor after a `SuspendReading`
   * command in order to resume reading from the socket.
   *
   * (This message is marked with DeadLetterSuppression as it is prone to end up in
   *  DeadLetters when the connection is torn down at the same time as the user wants
   *  to resume reading on that connection.)
   */
  case object ResumeReading extends Command with DeadLetterSuppression

  /**
   * This message enables the accepting of the next connection if read throttling is enabled
   * for connection actors.
   * @param batchSize The number of connections to accept before waiting for the next resume command
   */
  final case class ResumeAccepting(batchSize: Int) extends Command

  /// EVENTS
  /**
   * Common interface for all events generated by the TCP layer actors.
   */
  trait Event extends Message

  /**
   * Whenever data are read from a socket they will be transferred within this
   * class to the handler actor which was designated in the [[Register]] message.
   */
  final case class Received(data: ByteString) extends Event

  /**
   * The connection actor sends this message either to the sender of a [[Connect]]
   * command (for outbound) or to the handler for incoming connections designated
   * in the [[Bind]] message. The connection is characterized by the `remoteAddress`
   * and `localAddress` TCP endpoints.
   */
  final case class Connected(remoteAddress: InetSocketAddress, localAddress: InetSocketAddress) extends Event

  /**
   * Whenever a command cannot be completed, the queried actor will reply with
   * this message, wrapping the original command which failed.
   */
  final case class CommandFailed(cmd: Command) extends Event {
    @transient private var _cause: Option[Throwable] = None

    /** Optionally contains the cause why the command failed. */
    def cause: Option[Throwable] = _cause

    // Needs to be added with a mutable var for compatibility reasons.
    // The cause will be lost in the unlikely case that someone uses `copy` on an instance.
    /** Creates a copy of this object with a new cause set. */
    @InternalApi
    private[akka] def withCause(cause: Throwable): CommandFailed = {
      val newInstance = copy()
      newInstance._cause = Some(cause)
      newInstance
    }

    @InternalApi
    private[akka] def causedByString =
      _cause
        .map(t => {
          val msg =
            if (t.getCause == null)
              t.getMessage
            else if (t.getCause.getCause == null)
              s"${t.getMessage}, caused by: ${t.getCause}"
            else
              s"${t.getMessage}, caused by: ${t.getCause}, caused by: ${t.getCause.getCause}"

          s" because of ${t.getClass.getName}: $msg"
        })
        .getOrElse("")

    override def toString: String = s"CommandFailed($cmd)$causedByString"
  }

  /**
   * When `useResumeWriting` is in effect as indicated in the [[Register]] message,
   * the `ResumeWriting` command will be acknowledged by this message type, upon
   * which it is safe to send at least one write. This means that all writes preceding
   * the first [[CommandFailed]] message have been enqueued to the O/S kernel at this
   * point.
   */
  sealed trait WritingResumed extends Event
  case object WritingResumed extends WritingResumed

  /**
   * The sender of a [[Bind]] command will—in case of success—receive confirmation
   * in this form. If the bind address indicated a 0 port number, then the contained
   * `localAddress` can be used to find out which port was automatically assigned.
   */
  final case class Bound(localAddress: InetSocketAddress) extends Event

  /**
   * The sender of an `Unbind` command will receive confirmation through this
   * message once the listening socket has been closed.
   */
  sealed trait Unbound extends Event
  case object Unbound extends Unbound

  /**
   * This is the common interface for all events which indicate that a connection
   * has been closed or half-closed.
   */
  sealed trait ConnectionClosed extends Event with DeadLetterSuppression {

    /**
     * `true` iff the connection has been closed in response to an `Abort` command.
     */
    def isAborted: Boolean = false

    /**
     * `true` iff the connection has been fully closed in response to a
     * `ConfirmedClose` command.
     */
    def isConfirmed: Boolean = false

    /**
     * `true` iff the connection has been closed by the peer; in case
     * `keepOpenOnPeerClosed` is in effect as per the [[Register]] command,
     * this connection’s reading half is now closed.
     */
    def isPeerClosed: Boolean = false

    /**
     * `true` iff the connection has been closed due to an IO error.
     */
    def isErrorClosed: Boolean = false

    /**
     * If `isErrorClosed` returns true, then the error condition can be
     * retrieved by this method.
     */
    def getErrorCause: String = null
  }

  /**
   * The connection has been closed normally in response to a `Close` command.
   */
  case object Closed extends ConnectionClosed

  /**
   * The connection has been aborted in response to an `Abort` command.
   */
  case object Aborted extends ConnectionClosed {
    override def isAborted = true
  }

  /**
   * The connection has been half-closed by us and then half-close by the peer
   * in response to a `ConfirmedClose` command.
   */
  case object ConfirmedClosed extends ConnectionClosed {
    override def isConfirmed = true
  }

  /**
   * The peer has closed its writing half of the connection.
   */
  case object PeerClosed extends ConnectionClosed {
    override def isPeerClosed = true
  }

  /**
   * The connection has been closed due to an IO error.
   */
  final case class ErrorClosed(cause: String) extends ConnectionClosed {
    override def isErrorClosed = true
    override def getErrorCause = cause
  }
}

class TcpExt(system: ExtendedActorSystem) extends IO.Extension {

  val Settings = new Settings(system.settings.config.getConfig("akka.io.tcp"))
  class Settings private[TcpExt] (_config: Config) extends SelectionHandlerSettings(_config) {
    import _config._

    import akka.util.Helpers.ConfigOps

    val NrOfSelectors: Int = getInt("nr-of-selectors").requiring(_ > 0, "nr-of-selectors must be > 0")

    val BatchAcceptLimit: Int = getInt("batch-accept-limit").requiring(_ > 0, "batch-accept-limit must be > 0")
    val DirectBufferSize: Int = getIntBytes("direct-buffer-size")
    val MaxDirectBufferPoolSize: Int = getInt("direct-buffer-pool-limit")
    val RegisterTimeout: Duration = getString("register-timeout") match {
      case "infinite" => Duration.Undefined
      case _          => _config.getMillisDuration("register-timeout")
    }
    val ReceivedMessageSizeLimit: Int = getString("max-received-message-size") match {
      case "unlimited" => Int.MaxValue
      case _           => getIntBytes("max-received-message-size")
    }
    val ManagementDispatcher: String = getString("management-dispatcher")
    val FileIODispatcher: String = getString("file-io-dispatcher")
    val TransferToLimit: Int = getString("file-io-transferTo-limit") match {
      case "unlimited" => Int.MaxValue
      case _           => getIntBytes("file-io-transferTo-limit")
    }

    val MaxChannelsPerSelector: Int = if (MaxChannels == -1) -1 else math.max(MaxChannels / NrOfSelectors, 1)
    val FinishConnectRetries: Int =
      getInt("finish-connect-retries").requiring(_ > 0, "finish-connect-retries must be > 0")

    val WindowsConnectionAbortWorkaroundEnabled
        : Boolean = getString("windows-connection-abort-workaround-enabled") match {
      case "auto" => Helpers.isWindows
      case _      => getBoolean("windows-connection-abort-workaround-enabled")
    }

    private[this] def getIntBytes(path: String): Int = {
      val size = getBytes(path)
      require(size < Int.MaxValue, s"$path must be < 2 GiB")
      require(size >= 0, s"$path must be non-negative")
      size.toInt
    }
  }

  /**
   *
   */
  val manager: ActorRef = {
    system.systemActorOf(
      props = Props(new TcpManager(this)).withDispatcher(Settings.ManagementDispatcher).withDeploy(Deploy.local),
      name = "IO-TCP")
  }

  /**
   * Java API: retrieve a reference to the manager actor.
   */
  def getManager: ActorRef = manager

  val bufferPool: BufferPool = new DirectByteBufferPool(Settings.DirectBufferSize, Settings.MaxDirectBufferPoolSize)
  val fileIoDispatcher = system.dispatchers.lookup(Settings.FileIODispatcher)
}

/**
 * Java API for accessing socket options.
 */
object TcpSO extends SoJavaFactories {
  import Tcp.SO._

  /**
   * [[akka.io.Inet.SocketOption]] to enable or disable SO_KEEPALIVE
   *
   * For more information see `java.net.Socket.setKeepAlive`
   */
  def keepAlive(on: Boolean) = KeepAlive(on)

  /**
   * [[akka.io.Inet.SocketOption]] to enable or disable OOBINLINE (receipt
   * of TCP urgent data) By default, this option is disabled and TCP urgent
   * data is silently discarded.
   *
   * For more information see `java.net.Socket.setOOBInline`
   */
  def oobInline(on: Boolean) = OOBInline(on)

  /**
   * [[akka.io.Inet.SocketOption]] to enable or disable TCP_NODELAY
   * (disable or enable Nagle's algorithm)
   *
   * Please note, that TCP_NODELAY is enabled by default.
   *
   * For more information see `java.net.Socket.setTcpNoDelay`
   */
  def tcpNoDelay(on: Boolean) = TcpNoDelay(on)
}

object TcpMessage {
  import Tcp._
  import language.implicitConversions

  /**
   * The Connect message is sent to the TCP manager actor, which is obtained via
   * [[TcpExt#getManager]]. Either the manager replies with a [[Tcp.CommandFailed]]
   * or the actor handling the new connection replies with a [[Tcp.Connected]]
   * message.
   *
   * @param remoteAddress is the address to connect to
   * @param localAddress optionally specifies a specific address to bind to
   * @param options Please refer to [[TcpSO]] for a list of all supported options.
   * @param timeout is the desired connection timeout, `null` means "no timeout"
   * @param pullMode enables pull based reading from the connection
   */
  def connect(
      remoteAddress: InetSocketAddress,
      localAddress: InetSocketAddress,
      options: JIterable[SocketOption],
      timeout: FiniteDuration,
      pullMode: Boolean): Command =
    Connect(remoteAddress, Option(localAddress), options, Option(timeout), pullMode)

  /**
   * The Connect message is sent to the TCP manager actor, which is obtained via
   * [[TcpExt#getManager]]. Either the manager replies with a [[Tcp.CommandFailed]]
   * or the actor handling the new connection replies with a [[Tcp.Connected]]
   * message.
   *
   * @param remoteAddress is the address to connect to
   * @param localAddress optionally specifies a specific address to bind to
   * @param options Please refer to [[TcpSO]] for a list of all supported options.
   * @param timeout is the desired connection timeout, `null` means "no timeout"
   * @param pullMode enables pull based reading from the connection
   */
  def connect(
      remoteAddress: InetSocketAddress,
      localAddress: InetSocketAddress,
      options: JIterable[SocketOption],
      timeout: java.time.Duration,
      pullMode: Boolean): Command = connect(remoteAddress, localAddress, options, timeout.toScala, pullMode)

  /**
   * Connect to the given `remoteAddress` without binding to a local address and without
   * specifying options.
   */
  def connect(remoteAddress: InetSocketAddress): Command = Connect(remoteAddress, None, Nil, None, pullMode = false)

  /**
   * The Bind message is send to the TCP manager actor, which is obtained via
   * [[TcpExt#getManager]] in order to bind to a listening socket. The manager
   * replies either with a [[Tcp.CommandFailed]] or the actor handling the listen
   * socket replies with a [[Tcp.Bound]] message. If the local port is set to 0 in
   * the Bind message, then the [[Tcp.Bound]] message should be inspected to find
   * the actual port which was bound to.
   *
   * @param handler The actor which will receive all incoming connection requests
   *                in the form of [[Tcp.Connected]] messages.
   *
   * @param endpoint The socket address to bind to; use port zero for
   *                automatic assignment (i.e. an ephemeral port, see [[Tcp.Bound]])
   *
   * @param backlog This specifies the number of unaccepted connections the O/S
   *                kernel will hold for this port before refusing connections.
   *
   * @param options Please refer to [[TcpSO]] for a list of all supported options.
   *
   * @param pullMode enables pull based accepting and of connections and pull
   *                 based reading from the accepted connections.
   */
  def bind(
      handler: ActorRef,
      endpoint: InetSocketAddress,
      backlog: Int,
      options: JIterable[SocketOption],
      pullMode: Boolean): Command = Bind(handler, endpoint, backlog, options, pullMode)

  /**
   * Open a listening socket without specifying options.
   */
  def bind(handler: ActorRef, endpoint: InetSocketAddress, backlog: Int): Command =
    Bind(handler, endpoint, backlog, Nil)

  /**
   * This message must be sent to a TCP connection actor after receiving the
   * [[Tcp.Connected]] message. The connection will not read any data from the
   * socket until this message is received, because this message defines the
   * actor which will receive all inbound data.
   *
   * @param handler The actor which will receive all incoming data and which
   *                will be informed when the connection is closed.
   *
   * @param keepOpenOnPeerClosed If this is set to true then the connection
   *                is not automatically closed when the peer closes its half,
   *                requiring an explicit `Tcp.ConnectionClosed from our side when finished.
   *
   * @param useResumeWriting If this is set to true then the connection actor
   *                will refuse all further writes after issuing a [[Tcp.CommandFailed]]
   *                notification until [[Tcp]] `ResumeWriting` is received. This can
   *                be used to implement NACK-based write backpressure.
   */
  def register(handler: ActorRef, keepOpenOnPeerClosed: Boolean, useResumeWriting: Boolean): Command =
    Register(handler, keepOpenOnPeerClosed, useResumeWriting)

  /**
   * The same as `register(handler, false, false)`.
   */
  def register(handler: ActorRef): Command = Register(handler)

  /**
   * In order to close down a listening socket, send this message to that socket’s
   * actor (that is the actor which previously had sent the [[Tcp.Bound]] message). The
   * listener socket actor will reply with a `Tcp.Unbound` message.
   */
  def unbind: Command = Unbind

  /**
   * A normal close operation will first flush pending writes and then close the
   * socket. The sender of this command and the registered handler for incoming
   * data will both be notified once the socket is closed using a `Tcp.Closed`
   * message.
   */
  def close: Command = Close

  /**
   * A confirmed close operation will flush pending writes and half-close the
   * connection, waiting for the peer to close the other half. The sender of this
   * command and the registered handler for incoming data will both be notified
   * once the socket is closed using a `Tcp.ConfirmedClosed` message.
   */
  def confirmedClose: Command = ConfirmedClose

  /**
   * An abort operation will not flush pending writes and will issue a TCP ABORT
   * command to the O/S kernel which should result in a TCP_RST packet being sent
   * to the peer. The sender of this command and the registered handler for
   * incoming data will both be notified once the socket is closed using a
   * `Tcp.Aborted` message.
   */
  def abort: Command = Abort

  /**
   * Each [[Tcp.WriteCommand]] can optionally request a positive acknowledgment to be sent
   * to the commanding actor. If such notification is not desired the [[Tcp.SimpleWriteCommand#ack]]
   * must be set to an instance of this class. The token contained within can be used
   * to recognize which write failed when receiving a [[Tcp.CommandFailed]] message.
   */
  def noAck(token: AnyRef): NoAck = NoAck(token)

  /**
   * Default [[Tcp.NoAck]] instance which is used when no acknowledgment information is
   * explicitly provided. Its “token” is `null`.
   */
  def noAck: NoAck = NoAck

  /**
   * Write data to the TCP connection. If no ack is needed use the special
   * `NoAck` object. The connection actor will reply with a [[Tcp.CommandFailed]]
   * message if the write could not be enqueued. If [[Tcp.SimpleWriteCommand#wantsAck]]
   * returns true, the connection actor will reply with the supplied [[Tcp.SimpleWriteCommand#ack]]
   * token once the write has been successfully enqueued to the O/S kernel.
   * <b>Note that this does not in any way guarantee that the data will be
   * or have been sent!</b> Unfortunately there is no way to determine whether
   * a particular write has been sent by the O/S.
   */
  def write(data: ByteString, ack: Event): Command = Write(data, ack)

  /**
   * The same as `write(data, noAck())`.
   */
  def write(data: ByteString): Command = Write(data)

  /**
   * Write `count` bytes starting at `position` from file at `filePath` to the connection.
   * The count must be &gt; 0. The connection actor will reply with a [[Tcp.CommandFailed]]
   * message if the write could not be enqueued. If [[Tcp.SimpleWriteCommand#wantsAck]]
   * returns true, the connection actor will reply with the supplied [[Tcp.SimpleWriteCommand#ack]]
   * token once the write has been successfully enqueued to the O/S kernel.
   * <b>Note that this does not in any way guarantee that the data will be
   * or have been sent!</b> Unfortunately there is no way to determine whether
   * a particular write has been sent by the O/S.
   */
  def writeFile(filePath: String, position: Long, count: Long, ack: Event): Command =
    WritePath(Paths.get(filePath), position, count, ack)

  /**
   * Write `count` bytes starting at `position` from file at `filePath` to the connection.
   * The count must be &gt; 0. The connection actor will reply with a [[Tcp.CommandFailed]]
   * message if the write could not be enqueued. If [[Tcp.SimpleWriteCommand#wantsAck]]
   * returns true, the connection actor will reply with the supplied [[Tcp.SimpleWriteCommand#ack]]
   * token once the write has been successfully enqueued to the O/S kernel.
   * <b>Note that this does not in any way guarantee that the data will be
   * or have been sent!</b> Unfortunately there is no way to determine whether
   * a particular write has been sent by the O/S.
   */
  def writePath(filePath: Path, position: Long, count: Long, ack: Event): Command =
    WritePath(filePath, position, count, ack)

  /**
   * When `useResumeWriting` is in effect as was indicated in the [[Tcp.Register]] message
   * then this command needs to be sent to the connection actor in order to re-enable
   * writing after a [[Tcp.CommandFailed]] event. All [[Tcp.WriteCommand]] processed by the
   * connection actor between the first [[Tcp.CommandFailed]] and subsequent reception of
   * this message will also be rejected with [[Tcp.CommandFailed]].
   */
  def resumeWriting: Command = ResumeWriting

  /**
   * Sending this command to the connection actor will disable reading from the TCP
   * socket. TCP flow-control will then propagate backpressure to the sender side
   * as buffers fill up on either end. To re-enable reading send `Tcp.ResumeReading`.
   */
  def suspendReading: Command = SuspendReading

  /**
   * This command needs to be sent to the connection actor after a `Tcp.SuspendReading`
   * command in order to resume reading from the socket.
   */
  def resumeReading: Command = ResumeReading

  /**
   * This message enables the accepting of the next connection if pull reading is enabled
   * for connection actors.
   * @param batchSize The number of connections to accept before waiting for the next resume command
   */
  def resumeAccepting(batchSize: Int): Command = ResumeAccepting(batchSize)

  implicit private def fromJava[T](coll: JIterable[T]): immutable.Iterable[T] = {
    akka.japi.Util.immutableSeq(coll)
  }
}
