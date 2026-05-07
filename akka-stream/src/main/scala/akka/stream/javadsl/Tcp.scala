/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.lang.{ Iterable => JIterable }
import java.net.InetSocketAddress
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }
import java.util.function.Supplier

import scala.concurrent.ExecutionContext

import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSession
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.jdk.javaapi.DurationConverters
import scala.util.Failure
import scala.util.Success

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalApi
import akka.io.Inet.SocketOption
import akka.japi.Util.immutableSeq
import akka.stream.Materializer
import akka.stream.SystemMaterializer
import akka.stream.TLSClosing
import akka.stream.scaladsl
import akka.util.ByteString

object Tcp extends ExtensionId[Tcp] with ExtensionIdProvider {

  /**
   * Represents a prospective TCP server binding.
   *
   * Not indented for user construction
   */
  final class ServerBinding @InternalApi private[akka] (delegate: scaladsl.Tcp.ServerBinding) {

    /**
     * The local address of the endpoint bound by the materialization of the `connections` [[Source]].
     */
    def localAddress: InetSocketAddress = delegate.localAddress

    /**
     * Asynchronously triggers the unbinding of the port that was bound by the materialization of the `connections`
     * [[Source]].
     *
     * The produced [[java.util.concurrent.CompletionStage]] is fulfilled when the unbinding has been completed.
     */
    def unbind(): CompletionStage[Unit] = delegate.unbind().asJava

    /**
     * @return A completion operator that is completed when manually unbound, or failed if the server fails
     */
    def whenUnbound(): CompletionStage[Done] = delegate.whenUnbound.asJava
  }

  /**
   * Represents an accepted incoming TCP connection.
   */
  class IncomingConnection private[akka] (delegate: scaladsl.Tcp.IncomingConnection) {

    /**
     * The local address this connection is bound to.
     */
    def localAddress: InetSocketAddress = delegate.localAddress

    /**
     * The remote address this connection is bound to.
     */
    def remoteAddress: InetSocketAddress = delegate.remoteAddress

    /**
     * Handles the connection using the given flow, which is materialized exactly once and the respective
     * materialized value is returned.
     *
     * Convenience shortcut for: `flow.joinMat(handler, Keep.right).run(systemProvider)`.
     *
     * Note that the classic or typed `ActorSystem` can be used as the `systemProvider` parameter.
     */
    def handleWith[Mat](handler: Flow[ByteString, ByteString, Mat], systemProvider: ClassicActorSystemProvider): Mat =
      delegate.handleWith(handler.asScala)(SystemMaterializer(systemProvider.classicSystem).materializer)

    /**
     * Handles the connection using the given flow, which is materialized exactly once and the respective
     * materialized value is returned.
     *
     * Convenience shortcut for: `flow.joinMat(handler, Keep.right).run(materializer)`.
     *
     * Prefer the method taking an `ActorSystem` unless you have special requirements
     */
    def handleWith[Mat](handler: Flow[ByteString, ByteString, Mat], materializer: Materializer): Mat =
      delegate.handleWith(handler.asScala)(materializer)

    /**
     * A flow representing the client on the other side of the connection.
     * This flow can be materialized only once.
     */
    def flow: Flow[ByteString, ByteString, NotUsed] = new Flow(delegate.flow)
  }

  /**
   * Represents a prospective outgoing TCP connection.
   */
  class OutgoingConnection private[akka] (delegate: scaladsl.Tcp.OutgoingConnection) {

    /**
     * The remote address this connection is or will be bound to.
     */
    def remoteAddress: InetSocketAddress = delegate.remoteAddress

    /**
     * The local address of the endpoint bound by the materialization of the connection materialization.
     */
    def localAddress: InetSocketAddress = delegate.localAddress
  }

  override def get(system: ActorSystem): Tcp = super.get(system)

  override def get(system: ClassicActorSystemProvider): Tcp = super.get(system)

  def lookup = Tcp

  def createExtension(system: ExtendedActorSystem): Tcp = new Tcp(system)
}

class Tcp(system: ExtendedActorSystem) extends akka.actor.Extension {
  import Tcp._

  private lazy val delegate: scaladsl.Tcp = scaladsl.Tcp(system)

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
      backlog: Int,
      options: JIterable[SocketOption],
      halfClose: Boolean,
      idleTimeout: Optional[java.time.Duration]): Source[IncomingConnection, CompletionStage[ServerBinding]] =
    Source.fromGraph(
      delegate
        .bind(interface, port, backlog, immutableSeq(options), halfClose, optionalDurationToScala(idleTimeout))
        .map(new IncomingConnection(_))
        .mapMaterializedValue(_.map(new ServerBinding(_))(ExecutionContext.parasitic).asJava))

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
  @deprecated("Use bind that takes a java.time.Duration parameter instead.", "2.6.0")
  def bind(
      interface: String,
      port: Int,
      backlog: Int,
      options: JIterable[SocketOption],
      halfClose: Boolean,
      idleTimeout: Duration): Source[IncomingConnection, CompletionStage[ServerBinding]] =
    bind(interface, port, backlog, options, halfClose, durationToJavaOptional(idleTimeout))

  /**
   * Creates a [[Tcp.ServerBinding]] without specifying options.
   * It represents a prospective TCP server binding on the given `endpoint`.
   *
   * Please note that the startup of the server is asynchronous, i.e. after materializing the enclosing
   * [[akka.stream.scaladsl.RunnableGraph]] the server is not immediately available. Only after the materialized future
   * completes is the server ready to accept client connections.
   */
  def bind(interface: String, port: Int): Source[IncomingConnection, CompletionStage[ServerBinding]] =
    Source.fromGraph(
      delegate
        .bind(interface, port)
        .map(new IncomingConnection(_))
        .mapMaterializedValue(_.map(new ServerBinding(_))(ExecutionContext.parasitic).asJava))

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
      localAddress: Optional[InetSocketAddress],
      options: JIterable[SocketOption],
      halfClose: Boolean,
      connectTimeout: Optional[java.time.Duration],
      idleTimeout: Optional[java.time.Duration]): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] =
    Flow.fromGraph(
      delegate
        .outgoingConnection(
          remoteAddress,
          localAddress.toScala,
          immutableSeq(options),
          halfClose,
          optionalDurationToScala(connectTimeout),
          optionalDurationToScala(idleTimeout))
        .mapMaterializedValue(_.map(new OutgoingConnection(_))(ExecutionContext.parasitic).asJava))

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
  @deprecated("Use bind that takes a java.time.Duration parameter instead.", "2.6.0")
  def outgoingConnection(
      remoteAddress: InetSocketAddress,
      localAddress: Optional[InetSocketAddress],
      options: JIterable[SocketOption],
      halfClose: Boolean,
      connectTimeout: Duration,
      idleTimeout: Duration): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] =
    outgoingConnection(
      remoteAddress,
      localAddress,
      options,
      halfClose,
      durationToJavaOptional(connectTimeout),
      durationToJavaOptional(idleTimeout))

  /**
   * Creates an [[Tcp.OutgoingConnection]] without specifying options.
   * It represents a prospective TCP client connection to the given endpoint.
   *
   * Note that the ByteString chunk boundaries are not retained across the network,
   * to achieve application level chunks you have to introduce explicit framing in your streams,
   * for example using the [[Framing]] operators.
   */
  def outgoingConnection(host: String, port: Int): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] =
    Flow.fromGraph(
      delegate
        .outgoingConnection(new InetSocketAddress(host, port))
        .mapMaterializedValue(_.map(new OutgoingConnection(_))(ExecutionContext.parasitic).asJava))

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
      createSSLEngine: Supplier[SSLEngine]): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] =
    Flow.fromGraph(
      delegate
        .outgoingConnectionWithTls(remoteAddress, createSSLEngine = () => createSSLEngine.get())
        .mapMaterializedValue(_.map(new OutgoingConnection(_))(ExecutionContext.parasitic).asJava))

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
      createSSLEngine: Supplier[SSLEngine],
      localAddress: Optional[InetSocketAddress],
      options: JIterable[SocketOption],
      connectTimeout: Optional[java.time.Duration],
      idleTimeout: Optional[java.time.Duration],
      verifySession: JFunction[SSLSession, Optional[Throwable]],
      closing: TLSClosing): Flow[ByteString, ByteString, CompletionStage[OutgoingConnection]] = {
    Flow.fromGraph(
      delegate
        .outgoingConnectionWithTls(
          remoteAddress,
          createSSLEngine = () => createSSLEngine.get(),
          localAddress.toScala,
          immutableSeq(options),
          optionalDurationToScala(connectTimeout),
          optionalDurationToScala(idleTimeout),
          session =>
            verifySession.apply(session).toScala match {
              case None    => Success(())
              case Some(t) => Failure(t)
            },
          closing)
        .mapMaterializedValue(_.map(new OutgoingConnection(_))(ExecutionContext.parasitic).asJava))
  }

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * where all incoming and outgoing bytes are passed through TLS.
   *
   * @see [[Tcp.bind]]
   */
  def bindWithTls(
      interface: String,
      port: Int,
      createSSLEngine: Supplier[SSLEngine]): Source[IncomingConnection, CompletionStage[ServerBinding]] = {
    Source.fromGraph(
      delegate
        .bindWithTls(interface, port, createSSLEngine = () => createSSLEngine.get())
        .map(new IncomingConnection(_))
        .mapMaterializedValue(_.map(new ServerBinding(_))(ExecutionContext.parasitic).asJava))
  }

  /**
   * Creates a [[Tcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`
   * where all incoming and outgoing bytes are passed through TLS.
   *
   * @see [[Tcp.bind]]
   */
  def bindWithTls(
      interface: String,
      port: Int,
      createSSLEngine: Supplier[SSLEngine],
      backlog: Int,
      options: JIterable[SocketOption],
      idleTimeout: Optional[java.time.Duration],
      verifySession: JFunction[SSLSession, Optional[Throwable]],
      closing: TLSClosing): Source[IncomingConnection, CompletionStage[ServerBinding]] = {
    Source.fromGraph(
      delegate
        .bindWithTls(
          interface,
          port,
          createSSLEngine = () => createSSLEngine.get(),
          backlog,
          immutableSeq(options),
          optionalDurationToScala(idleTimeout),
          session =>
            verifySession.apply(session).toScala match {
              case None    => Success(())
              case Some(t) => Failure(t)
            },
          closing)
        .map(new IncomingConnection(_))
        .mapMaterializedValue(_.map(new ServerBinding(_))(ExecutionContext.parasitic).asJava))
  }

  private def optionalDurationToScala(duration: Optional[java.time.Duration]) = {
    if (duration.isPresent) duration.get.toScala else Duration.Inf
  }

  private def durationToJavaOptional(duration: Duration): Optional[java.time.Duration] = {
    duration match {
      case f: FiniteDuration => Optional.ofNullable(DurationConverters.toJava(f))
      case _                 => Optional.empty()
    }
  }
}
