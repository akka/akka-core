/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.util.Optional
import java.util.function.{ Consumer, Supplier }
import javax.net.ssl.{ SSLContext, SSLEngine, SSLSession }

import scala.compat.java8.OptionConverters
import scala.util.Try

import com.typesafe.sslconfig.akka.AkkaSSLConfig

import akka.{ japi, NotUsed }
import akka.stream._
import akka.stream.TLSProtocol._
import akka.util.ByteString

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
   * Create a StreamTls [[akka.stream.javadsl.BidiFlow]] in client mode. The
   * SSLContext will be used to create an SSLEngine to which then the
   * `firstSession` parameters are applied before initiating the first
   * handshake. The `role` parameter determines the SSLEngine’s role; this is
   * often the same as the underlying transport’s server or client role, but
   * that is not a requirement and depends entirely on the application
   * protocol.
   *
   * This method uses the default closing behavior or [[IgnoreComplete]].
   */
  @deprecated("Use create that takes a SSLEngine factory instead. Setup the SSLEngine with needed parameters.", "2.6.0")
  def create(
      sslContext: SSLContext,
      sslConfig: Optional[AkkaSSLConfig],
      firstSession: NegotiateNewSession,
      role: TLSRole): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    new javadsl.BidiFlow(scaladsl.TLS.apply(sslContext, OptionConverters.toScala(sslConfig), firstSession, role))

  /**
   * Create a StreamTls [[akka.stream.javadsl.BidiFlow]] in client mode. The
   * SSLContext will be used to create an SSLEngine to which then the
   * `firstSession` parameters are applied before initiating the first
   * handshake. The `role` parameter determines the SSLEngine’s role; this is
   * often the same as the underlying transport’s server or client role, but
   * that is not a requirement and depends entirely on the application
   * protocol.
   *
   * This method uses the default closing behavior or [[IgnoreComplete]].
   */
  @deprecated("Use create that takes a SSLEngine factory instead. Setup the SSLEngine with needed parameters.", "2.6.0")
  def create(
      sslContext: SSLContext,
      firstSession: NegotiateNewSession,
      role: TLSRole): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    new javadsl.BidiFlow(scaladsl.TLS.apply(sslContext, None, firstSession, role))

  /**
   * Create a StreamTls [[akka.stream.javadsl.BidiFlow]] in client mode. The
   * SSLContext will be used to create an SSLEngine to which then the
   * `firstSession` parameters are applied before initiating the first
   * handshake. The `role` parameter determines the SSLEngine’s role; this is
   * often the same as the underlying transport’s server or client role, but
   * that is not a requirement and depends entirely on the application
   * protocol.
   *
   * For a description of the `closing` parameter please refer to [[TLSClosing]].
   *
   * The `hostInfo` parameter allows to optionally specify a pair of hostname and port
   * that will be used when creating the SSLEngine with `sslContext.createSslEngine`.
   * The SSLEngine may use this information e.g. when an endpoint identification algorithm was
   * configured using [[javax.net.ssl.SSLParameters.setEndpointIdentificationAlgorithm]].
   */
  @deprecated("Use create that takes a SSLEngine factory instead. Setup the SSLEngine with needed parameters.", "2.6.0")
  def create(
      sslContext: SSLContext,
      sslConfig: Optional[AkkaSSLConfig],
      firstSession: NegotiateNewSession,
      role: TLSRole,
      hostInfo: Optional[japi.Pair[String, java.lang.Integer]],
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    new javadsl.BidiFlow(
      scaladsl.TLS.apply(
        sslContext,
        OptionConverters.toScala(sslConfig),
        firstSession,
        role,
        closing,
        OptionConverters.toScala(hostInfo).map(e => (e.first, e.second))))

  /**
   * Create a StreamTls [[akka.stream.javadsl.BidiFlow]] in client mode. The
   * SSLContext will be used to create an SSLEngine to which then the
   * `firstSession` parameters are applied before initiating the first
   * handshake. The `role` parameter determines the SSLEngine’s role; this is
   * often the same as the underlying transport’s server or client role, but
   * that is not a requirement and depends entirely on the application
   * protocol.
   *
   * For a description of the `closing` parameter please refer to [[TLSClosing]].
   *
   * The `hostInfo` parameter allows to optionally specify a pair of hostname and port
   * that will be used when creating the SSLEngine with `sslContext.createSslEngine`.
   * The SSLEngine may use this information e.g. when an endpoint identification algorithm was
   * configured using [[javax.net.ssl.SSLParameters.setEndpointIdentificationAlgorithm]].
   */
  @deprecated("Use create that takes a SSLEngine factory instead. Setup the SSLEngine with needed parameters.", "2.6.0")
  def create(
      sslContext: SSLContext,
      firstSession: NegotiateNewSession,
      role: TLSRole,
      hostInfo: Optional[japi.Pair[String, java.lang.Integer]],
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    new javadsl.BidiFlow(
      scaladsl.TLS.apply(
        sslContext,
        None,
        firstSession,
        role,
        closing,
        OptionConverters.toScala(hostInfo).map(e => (e.first, e.second))))

  /**
   * Create a StreamTls [[akka.stream.javadsl.BidiFlow]]. This is a low-level interface.
   *
   * You specify a factory `sslEngineCreator` to create an SSLEngine that must already be configured for
   * client and server mode and with all the parameters for the first session.
   *
   * You can specify a verification function `sessionVerifier` that will be called
   * after every successful handshake to verify additional session information.
   *
   * For a description of the `closing` parameter please refer to [[TLSClosing]].
   */
  def create(
      sslEngineCreator: Supplier[SSLEngine],
      sessionVerifier: Consumer[SSLSession],
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    new javadsl.BidiFlow(
      scaladsl.TLS.apply(() => sslEngineCreator.get(), session => Try(sessionVerifier.accept(session)), closing))

  /**
   * Create a StreamTls [[akka.stream.javadsl.BidiFlow]]. This is a low-level interface.
   *
   * You specify a factory `sslEngineCreator` to create an SSLEngine that must already be configured for
   * client and server mode and with all the parameters for the first session.
   *
   * For a description of the `closing` parameter please refer to [[TLSClosing]].
   */
  def create(
      sslEngineCreator: Supplier[SSLEngine],
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    new javadsl.BidiFlow(scaladsl.TLS.apply(() => sslEngineCreator.get(), closing))
}

/**
 * This object holds simple wrapping [[akka.stream.scaladsl.BidiFlow]] implementations that can
 * be used instead of [[TLS]] when no encryption is desired. The flows will
 * just adapt the message protocol by wrapping into [[SessionBytes]] and
 * unwrapping [[SendBytes]].
 */
object TLSPlacebo {

  /**
   * Returns a reusable [[BidiFlow]] instance representing a [[TLSPlacebo$]].
   */
  def getInstance(): javadsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SessionBytes, NotUsed] = forJava

  private val forJava: javadsl.BidiFlow[SslTlsOutbound, ByteString, ByteString, SessionBytes, NotUsed] =
    new javadsl.BidiFlow(scaladsl.TLSPlacebo())
}
