/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.NotUsed
import akka.stream.TLSProtocol._
import akka.stream._
import akka.stream.impl.io.TlsModule
import akka.util.ByteString

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSession
import scala.util.Success
import scala.util.Try

/**
 * Stream cipher support based upon JSSE.
 *
 * The underlying SSLEngine has four ports: plaintext input/output and
 * ciphertext input/output. These are modeled as a [[akka.stream.BidiShape]]
 * element for use in stream topologies, where the plaintext ports are on the
 * left hand side of the shape and the ciphertext ports on the right hand side.
 *
 * Configuring JSSE is a rather complex topic, please refer to the JDK platform
 * documentation or the excellent user guide that is part of the Play Framework
 * documentation. The philosophy of this integration into Akka Streams is to
 * expose all knobs and dials to client code and therefore not limit the
 * configuration possibilities. In particular the client code will have to
 * provide the SSLEngine, which is typically created from a SSLContext. Handshake
 * parameters and other parameters are defined when creating the SSLEngine.
 *
 * '''IMPORTANT NOTE'''
 *
 * The TLS specification until version 1.2 did not permit half-closing of the user data session
 * that it transports—to be precise a half-close will always promptly lead to a
 * full close. This means that canceling the plaintext output or completing the
 * plaintext input of the SslTls operator will lead to full termination of the
 * secure connection without regard to whether bytes are remaining to be sent or
 * received, respectively. Especially for a client the common idiom of attaching
 * a finite Source to the plaintext input and transforming the plaintext response
 * bytes coming out will not work out of the box due to early termination of the
 * connection. For this reason there is a parameter that determines whether the
 * SslTls operator shall ignore completion and/or cancellation events, and the
 * default is to ignore completion (in view of the client–server scenario). In
 * order to terminate the connection the client will then need to cancel the
 * plaintext output as soon as all expected bytes have been received. When
 * ignoring both types of events the operator will shut down once both events have
 * been received. See also [[TLSClosing]]. For now, half-closing is also not
 * supported with TLS 1.3 where the spec allows it.
 */
object TLS {

  /**
   * Create a StreamTls [[akka.stream.scaladsl.BidiFlow]].
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * client and server mode and with all the parameters for the first session.
   *
   * You can specify a verification function that will be called after every successful handshake
   * to verify additional session information.
   *
   * For a description of the `closing` parameter please refer to [[TLSClosing]].
   */
  def apply(
      createSSLEngine: () => SSLEngine, // we don't offer the internal `ActorSystem => SSLEngine` API here, see #21753
      verifySession: SSLSession => Try[Unit], // we don't offer the internal API that provides `ActorSystem` here, see #21753
      closing: TLSClosing): scaladsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    scaladsl.BidiFlow.fromGraph(TlsModule(Attributes.none, createSSLEngine, verifySession, closing))

  /**
   * Create a StreamTls [[akka.stream.scaladsl.BidiFlow]].
   *
   * You specify a factory to create an SSLEngine that must already be configured for
   * client and server mode and with all the parameters for the first session.
   *
   * For a description of the `closing` parameter please refer to [[TLSClosing]].
   */
  def apply(
      createSSLEngine: () => SSLEngine, // we don't offer the internal `ActorSystem => SSLEngine` API here, see #21753
      closing: TLSClosing): scaladsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    apply(createSSLEngine, _ => Success(()), closing)
}

/**
 * This object holds simple wrapping [[akka.stream.scaladsl.BidiFlow]] implementations that can
 * be used instead of [[TLS]] when no encryption is desired. The flows will
 * just adapt the message protocol by wrapping into [[SessionBytes]] and
 * unwrapping [[SendBytes]].
 */
object TLSPlacebo {
  // this constructs a session for (invalid) protocol SSL_NULL_WITH_NULL_NULL
  private[akka] val dummySession = SSLContext.getDefault.createSSLEngine.getSession

  def apply(): scaladsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SessionBytes, NotUsed] = instance

  private val instance: scaladsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SessionBytes, NotUsed] =
    scaladsl.BidiFlow.fromGraph(scaladsl.GraphDSL.create() { implicit b =>
      val top = b.add(scaladsl.Flow[SslTlsOutbound].collect { case SendBytes(bytes) => bytes })
      val bottom = b.add(scaladsl.Flow[ByteString].map(SessionBytes(dummySession, _)))
      BidiShape.fromFlows(top, bottom)
    })
}

import java.security.Principal
import java.security.cert.Certificate
import javax.net.ssl.SSLPeerUnverifiedException

/** Allows access to an SSLSession with Scala types */
trait ScalaSessionAPI {

  /**
   * The underlying [[javax.net.ssl.SSLSession]].
   */
  def session: SSLSession

  /**
   * Scala API: Extract the certificates that were actually used by this
   * engine during this session’s negotiation. The list is empty if no
   * certificates were used.
   */
  def localCertificates: List[Certificate] = Option(session.getLocalCertificates).map(_.toList).getOrElse(Nil)

  /**
   * Scala API: Extract the Principal that was actually used by this engine
   * during this session’s negotiation.
   */
  def localPrincipal: Option[Principal] = Option(session.getLocalPrincipal)

  /**
   * Scala API: Extract the certificates that were used by the peer engine
   * during this session’s negotiation. The list is empty if no certificates
   * were used.
   */
  def peerCertificates: List[Certificate] =
    try Option(session.getPeerCertificates).map(_.toList).getOrElse(Nil)
    catch { case _: SSLPeerUnverifiedException => Nil }

  /**
   * Scala API: Extract the Principal that the peer engine presented during
   * this session’s negotiation.
   */
  def peerPrincipal: Option[Principal] =
    try Option(session.getPeerPrincipal)
    catch { case _: SSLPeerUnverifiedException => None }
}

object ScalaSessionAPI {

  /** Constructs a ScalaSessionAPI instance from an SSLSession */
  def apply(_session: SSLSession): ScalaSessionAPI =
    new ScalaSessionAPI {
      def session: SSLSession = _session
    }
}
