/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.net.InetSocketAddress
import java.util.concurrent.TimeoutException
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSession

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.Try
import scala.util.control.NoStackTrace

import akka.Done
import akka.NotUsed
import akka.actor._
import akka.annotation.InternalApi
import akka.io.{ Tcp => IoTcp }
import akka.io.IO
import akka.io.Inet.SocketOption
import akka.stream._
import akka.stream.Attributes.Attribute
import akka.stream.TLSProtocol.NegotiateNewSession
import akka.stream.impl.fusing.GraphStages.detacher
import akka.stream.impl.io.ConnectionSourceStage
import akka.stream.impl.io.OutgoingConnectionStage
import akka.stream.impl.io.TcpIdleTimeout
import akka.util.ByteString
import akka.util.JavaDurationConverters._
import akka.util.unused

object Tcp extends ExtensionId[Tcp] with ExtensionIdProvider {

  /**
   * Represents a successful TCP server binding.
   *
   * Not indented for user construction
   *
   * @param localAddress The address the server was bound to
   * @param unbindAction a function that will trigger unbind of the server
   * @param whenUnbound A future that is completed when the server is unbound, or failed if the server binding fails
   */
  final case class ServerBinding @InternalApi private[akka] (localAddress: InetSocketAddress)(
      private val unbindAction: () => Future[Unit],
      val whenUnbound: Future[Done]) {
    def unbind(): Future[Unit] = unbindAction()
  }

  /**
   * Represents an accepted incoming TCP connection.
   */
  final case class IncomingConnection(
      localAddress: InetSocketAddress,
      remoteAddress: InetSocketAddress,
      flow: Flow[ByteString, ByteString, NotUsed]) {

    /**
     * Handles the connection using the given flow, which is materialized exactly once and the respective
     * materialized instance is returned.
     *
     * Convenience shortcut for: `flow.joinMat(handler)(Keep.right).run()`.
     */
    def handleWith[Mat](handler: Flow[ByteString, ByteString, Mat])(implicit materializer: Materializer): Mat =
      flow.joinMat(handler)(Keep.right).run()

  }

  /**
   * Represents a prospective outgoing TCP connection.
   */
  final case class OutgoingConnection(remoteAddress: InetSocketAddress, localAddress: InetSocketAddress)

  def apply()(implicit system: ActorSystem): Tcp = super.apply(system)

  override def get(system: ActorSystem): Tcp = super.get(system)
  override def get(system: ClassicActorSystemProvider): Tcp = super.get(system)

  def lookup = Tcp

  def createExtension(system: ExtendedActorSystem): Tcp = new Tcp(system)

  // just wraps/unwraps the TLS byte events to provide ByteString, ByteString flows
  private val tlsWrapping: BidiFlow[ByteString, TLSProtocol.SendBytes, TLSProtocol.SslTlsInbound, ByteString, NotUsed] =
    BidiFlow.fromFlows(Flow[ByteString].map(TLSProtocol.SendBytes.apply), Flow[TLSProtocol.SslTlsInbound].collect {
      case sb: TLSProtocol.SessionBytes => sb.bytes
      // ignore other kinds of inbounds (currently only Truncated)
    })

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val defaultBacklog = 100
}

final class Tcp(system: ExtendedActorSystem) extends akka.actor.Extension {
  import Tcp._

  // TODO maybe this should be a new setting, like `akka.stream.tcp.bind.timeout` / `shutdown-timeout` instead?
  val bindShutdownTimeout: FiniteDuration =
    system.settings.config.getDuration("akka.stream.materializer.subscription-timeout.timeout").asScala

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the materialized future
   * completes is the server ready to accept client connections.
   *
   * @param interface The interface to listen on
   * @param port      The port to listen on
   * @param backlog   Controls the size of the connection backlog
   * @param options   TCP options for the connections, see [[akka.io.Tcp]] for details
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  TCP connections.
   *                  If set to true, the connection will implement the TCP half-close mechanism, allowing the client to
   *                  write to the connection even after the server has finished writing. The TCP socket is only closed
   *                  after both the client and server finished writing.
   *                  If set to false, the connection will immediately closed once the server closes its write side,
   *                  independently whether the client is still attempting to write. This setting is recommended
   *                  for servers, and therefore it is the default setting.
   */
  def bind(
      interface: String,
      port: Int,
      backlog: Int = defaultBacklog,
      @nowarn // Traversable deprecated in 2.13
      options: immutable.Traversable[SocketOption] = Nil,
      halfClose: Boolean = false,
      idleTimeout: Duration = Duration.Inf): Source[IncomingConnection, Future[ServerBinding]] =
    Source.fromGraph(
      new ConnectionSourceStage(
        IO(IoTcp)(system),
        new InetSocketAddress(interface, port),
        backlog,
        options.toList,
        halfClose,
        idleTimeout,
        bindShutdownTimeout))

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * handling the incoming connections using the provided Flow.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the returned future
   * completes is the server ready to accept client connections.
   *
   * @param handler   A Flow that represents the server logic
   * @param interface The interface to listen on
   * @param port      The port to listen on
   * @param backlog   Controls the size of the connection backlog
   * @param options   TCP options for the connections, see [[akka.io.Tcp]] for details
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  TCP connections.
   *                  If set to true, the connection will implement the TCP half-close mechanism, allowing the client to
   *                  write to the connection even after the server has finished writing. The TCP socket is only closed
   *                  after both the client and server finished writing.
   *                  If set to false, the connection will immediately closed once the server closes its write side,
   *                  independently whether the client is still attempting to write. This setting is recommended
   *                  for servers, and therefore it is the default setting.
   */
  def bindAndHandle(
      handler: Flow[ByteString, ByteString, _],
      interface: String,
      port: Int,
      backlog: Int = defaultBacklog,
      @nowarn // Traversable deprecated in 2.13
      options: immutable.Traversable[SocketOption] = Nil,
      halfClose: Boolean = false,
      idleTimeout: Duration = Duration.Inf)(implicit m: Materializer): Future[ServerBinding] = {
    bind(interface, port, backlog, options, halfClose, idleTimeout)
      .to(Sink.foreach { (conn: IncomingConnection) =>
        conn.flow.join(handler).run()
      })
      .run()
  }

  /**
   * Creates an [[Tcp.OutgoingConnection]] instance representing a prospective TCP client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[Framing]] operators.
   *
   * @param remoteAddress The remote address to connect to
   * @param localAddress  Optional local address for the connection
   * @param options   TCP options for the connections, see [[akka.io.Tcp]] for details
   * @param halfClose
   *                  Controls whether the connection is kept open even after writing has been completed to the accepted
   *                  TCP connections.
   *                  If set to true, the connection will implement the TCP half-close mechanism, allowing the server to
   *                  write to the connection even after the client has finished writing. The TCP socket is only closed
   *                  after both the client and server finished writing. This setting is recommended for clients and
   *                  therefore it is the default setting.
   *                  If set to false, the connection will immediately closed once the client closes its write side,
   *                  independently whether the server is still attempting to write.
   */
  def outgoingConnection(
      remoteAddress: InetSocketAddress,
      localAddress: Option[InetSocketAddress] = None,
      @nowarn // Traversable deprecated in 2.13
      options: immutable.Traversable[SocketOption] = Nil,
      halfClose: Boolean = true,
      connectTimeout: Duration = Duration.Inf,
      idleTimeout: Duration = Duration.Inf): Flow[ByteString, ByteString, Future[OutgoingConnection]] = {

    val tcpFlow = Flow
      .fromGraph(
        new OutgoingConnectionStage(
          IO(IoTcp)(system),
          remoteAddress,
          localAddress,
          options.toList,
          halfClose,
          connectTimeout))
      .via(detacher[ByteString]) // must read ahead for proper completions

    idleTimeout match {
      case d: FiniteDuration => tcpFlow.join(TcpIdleTimeout(d, Some(remoteAddress)))
      case _                 => tcpFlow
    }

  }

  /**
   * Creates an [[Tcp.OutgoingConnection]] without specifying options.
   * It represents a prospective TCP client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[Framing]] operators.
   */
  def outgoingConnection(host: String, port: Int): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    outgoingConnection(InetSocketAddress.createUnresolved(host, port))

  /**
   * Creates an [[Tcp.OutgoingConnection]] with TLS.
   * The returned flow represents a TCP client connection to the given endpoint where all bytes in and
   * out go through TLS.
   *
   * For more advanced use cases you can manually combine [[Tcp.outgoingConnection]] and [[TLS]]
   *
   * @param negotiateNewSession Details about what to require when negotiating the connection with the server
   * @param sslContext Context containing details such as the trust and keystore
   *
   * @see [[Tcp.outgoingConnection]]
   */
  @deprecated(
    "Use outgoingConnectionWithTls that takes a SSLEngine factory instead. " +
    "Setup the SSLEngine with needed parameters.",
    "2.6.0")
  def outgoingTlsConnection(
      host: String,
      port: Int,
      sslContext: SSLContext,
      negotiateNewSession: NegotiateNewSession): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    outgoingTlsConnection(InetSocketAddress.createUnresolved(host, port), sslContext, negotiateNewSession)

  /**
   * Creates an [[Tcp.OutgoingConnection]] with TLS.
   * The returned flow represents a TCP client connection to the given endpoint where all bytes in and
   * out go through TLS.
   *
   * @see [[Tcp.outgoingConnection]]
   * @param negotiateNewSession Details about what to require when negotiating the connection with the server
   * @param sslContext Context containing details such as the trust and keystore
   */
  @deprecated(
    "Use outgoingConnectionWithTls that takes a SSLEngine factory instead. " +
    "Setup the SSLEngine with needed parameters.",
    "2.6.0")
  def outgoingTlsConnection(
      remoteAddress: InetSocketAddress,
      sslContext: SSLContext,
      negotiateNewSession: NegotiateNewSession,
      localAddress: Option[InetSocketAddress] = None,
      @nowarn // Traversable deprecated in 2.13
      options: immutable.Traversable[SocketOption] = Nil,
      connectTimeout: Duration = Duration.Inf,
      idleTimeout: Duration = Duration.Inf): Flow[ByteString, ByteString, Future[OutgoingConnection]] = {

    val connection = outgoingConnection(remoteAddress, localAddress, options, true, connectTimeout, idleTimeout)
    @nowarn("msg=deprecated")
    val tls = TLS(sslContext, negotiateNewSession, TLSRole.client)
    connection.join(tlsWrapping.atop(tls).reversed)
  }

  /**
   * Creates an [[Tcp.OutgoingConnection]] with TLS.
   * The returned flow represents a TCP client connection to the given endpoint where all bytes in and
   * out go through TLS.
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * client mode and with all the parameters for the first session.
   *
   * @see [[Tcp.outgoingConnection]]
   */
  def outgoingConnectionWithTls(
      remoteAddress: InetSocketAddress,
      createSSLEngine: () => SSLEngine): Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    outgoingConnectionWithTls(
      remoteAddress,
      createSSLEngine,
      localAddress = None,
      options = Nil,
      connectTimeout = Duration.Inf,
      idleTimeout = Duration.Inf,
      verifySession = _ => Success(()),
      closing = IgnoreComplete)

  /**
   * Creates an [[Tcp.OutgoingConnection]] with TLS.
   * The returned flow represents a TCP client connection to the given endpoint where all bytes in and
   * out go through TLS.
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * client mode and with all the parameters for the first session.
   *
   * @see [[Tcp.outgoingConnection]]
   */
  def outgoingConnectionWithTls(
      remoteAddress: InetSocketAddress,
      createSSLEngine: () => SSLEngine,
      localAddress: Option[InetSocketAddress],
      options: immutable.Seq[SocketOption],
      connectTimeout: Duration,
      idleTimeout: Duration,
      verifySession: SSLSession => Try[Unit],
      closing: TLSClosing): Flow[ByteString, ByteString, Future[OutgoingConnection]] = {

    val connection = outgoingConnection(remoteAddress, localAddress, options, true, connectTimeout, idleTimeout)
    val tls = TLS(createSSLEngine, verifySession, closing)
    connection.join(tlsWrapping.atop(tls).reversed)
  }

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * where all incoming and outgoing bytes are passed through TLS.
   *
   * @param negotiateNewSession Details about what to require when negotiating the connection with the server
   * @param sslContext Context containing details such as the trust and keystore
   * @see [[Tcp.bind]]
   */
  @deprecated(
    "Use bindWithTls that takes a SSLEngine factory instead. " +
    "Setup the SSLEngine with needed parameters.",
    "2.6.0")
  def bindTls(
      interface: String,
      port: Int,
      sslContext: SSLContext,
      negotiateNewSession: NegotiateNewSession,
      backlog: Int = defaultBacklog,
      @nowarn // Traversable deprecated in 2.13
      options: immutable.Traversable[SocketOption] = Nil,
      idleTimeout: Duration = Duration.Inf): Source[IncomingConnection, Future[ServerBinding]] = {
    @nowarn("msg=deprecated")
    val tls = tlsWrapping.atop(TLS(sslContext, negotiateNewSession, TLSRole.server)).reversed

    bind(interface, port, backlog, options, halfClose = false, idleTimeout).map { incomingConnection =>
      incomingConnection.copy(flow = incomingConnection.flow.join(tls))
    }
  }

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * where all incoming and outgoing bytes are passed through TLS.
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * server mode and with all the parameters for the first session.
   *
   * @see [[Tcp.bind]]
   */
  def bindWithTls(
      interface: String,
      port: Int,
      createSSLEngine: () => SSLEngine): Source[IncomingConnection, Future[ServerBinding]] =
    bindWithTls(
      interface,
      port,
      createSSLEngine,
      backlog = defaultBacklog,
      options = Nil,
      idleTimeout = Duration.Inf,
      verifySession = _ => Success(()),
      closing = IgnoreComplete)

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * where all incoming and outgoing bytes are passed through TLS.
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * server mode and with all the parameters for the first session.
   *
   * @see [[Tcp.bind]]
   */
  def bindWithTls(
      interface: String,
      port: Int,
      createSSLEngine: () => SSLEngine,
      backlog: Int,
      options: immutable.Seq[SocketOption],
      idleTimeout: Duration,
      verifySession: SSLSession => Try[Unit],
      closing: TLSClosing): Source[IncomingConnection, Future[ServerBinding]] = {

    val tls = tlsWrapping.atop(TLS(createSSLEngine, verifySession, closing)).reversed

    bind(interface, port, backlog, options, halfClose = true, idleTimeout).map { incomingConnection =>
      incomingConnection.copy(flow = incomingConnection.flow.join(tls))
    }
  }

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * all incoming and outgoing bytes are passed through TLS and handling the incoming connections using the
   * provided Flow.
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * server mode and with all the parameters for the first session.
   *
   * @see [[Tcp.bindAndHandle]]
   */
  def bindAndHandleWithTls(
      handler: Flow[ByteString, ByteString, _],
      interface: String,
      port: Int,
      createSSLEngine: () => SSLEngine)(implicit m: Materializer): Future[ServerBinding] =
    bindAndHandleWithTls(
      handler,
      interface,
      port,
      createSSLEngine,
      backlog = defaultBacklog,
      options = Nil,
      idleTimeout = Duration.Inf,
      verifySession = _ => Success(()),
      closing = IgnoreComplete)

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * all incoming and outgoing bytes are passed through TLS and handling the incoming connections using the
   * provided Flow.
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * server mode and with all the parameters for the first session.
   *
   * @see [[Tcp.bindAndHandle]]
   */
  def bindAndHandleWithTls(
      handler: Flow[ByteString, ByteString, _],
      interface: String,
      port: Int,
      createSSLEngine: () => SSLEngine,
      backlog: Int,
      options: immutable.Seq[SocketOption],
      idleTimeout: Duration,
      verifySession: SSLSession => Try[Unit],
      closing: TLSClosing)(implicit m: Materializer): Future[ServerBinding] = {
    bindWithTls(interface, port, createSSLEngine, backlog, options, idleTimeout, verifySession, closing)
      .to(Sink.foreach { (conn: IncomingConnection) =>
        conn.handleWith(handler)
      })
      .run()
  }

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * handling the incoming connections through TLS and then run using the provided Flow.
   *
   * @param negotiateNewSession Details about what to require when negotiating the connection with the server
   * @param sslContext Context containing details such as the trust and keystore
   * @see [[Tcp.bindAndHandle]]
   *
   * Marked API-may-change to leave room for an improvement around the very long parameter list.
   */
  @deprecated(
    "Use bindAndHandleWithTls that takes a SSLEngine factory instead. " +
    "Setup the SSLEngine with needed parameters.",
    "2.6.0")
  def bindAndHandleTls(
      handler: Flow[ByteString, ByteString, _],
      interface: String,
      port: Int,
      sslContext: SSLContext,
      negotiateNewSession: NegotiateNewSession,
      backlog: Int = defaultBacklog,
      @nowarn // Traversable deprecated in 2.13
      options: immutable.Traversable[SocketOption] = Nil,
      idleTimeout: Duration = Duration.Inf)(implicit m: Materializer): Future[ServerBinding] = {
    bindTls(interface, port, sslContext, negotiateNewSession, backlog, options, idleTimeout)
      .to(Sink.foreach { (conn: IncomingConnection) =>
        conn.handleWith(handler)
      })
      .run()
  }

}

final class TcpIdleTimeoutException(msg: String, @unused timeout: Duration)
    extends TimeoutException(msg: String)
    with NoStackTrace // only used from a single stage

object TcpAttributes {
  final case class TcpWriteBufferSize(size: Int) extends Attribute {
    require(size > 0)
  }
  def tcpWriteBufferSize(size: Int): Attributes =
    Attributes(TcpWriteBufferSize(size))
}
