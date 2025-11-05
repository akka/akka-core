/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import javax.net.ssl._

import scala.annotation.varargs
import scala.collection.immutable

import akka.util.ByteString

/**
 * Many protocols are asymmetric and distinguish between the client and the
 * server, where the latter listens passively for messages and the former
 * actively initiates the exchange.
 */
object TLSRole {

  /**
   * Java API: obtain the [[Client]] singleton value.
   */
  def client: TLSRole = Client

  /**
   * Java API: obtain the [[Server]] singleton value.
   */
  def server: TLSRole = Server
}
sealed abstract class TLSRole

/**
 * The client is usually the side that consumes the service provided by its
 * interlocutor. The precise interpretation of this role is protocol specific.
 */
sealed abstract class Client extends TLSRole
case object Client extends Client

/**
 * The server is usually the side the provides the service to its interlocutor.
 * The precise interpretation of this role is protocol specific.
 */
sealed abstract class Server extends TLSRole
case object Server extends Server

/**
 * All streams in Akka are unidirectional: while in a complex flow graph data
 * may flow in multiple directions these individual flows are independent from
 * each other. The difference between two half-duplex connections in opposite
 * directions and a full-duplex connection is that the underlying transport
 * is shared in the latter and tearing it down will end the data transfer in
 * both directions.
 *
 * When integrating a full-duplex transport medium that does not support
 * half-closing (which means ending one direction of data transfer without
 * ending the other) into a stream topology, there can be unexpected effects.
 * Feeding a finite Source into this medium will close the connection after
 * all elements have been sent, which means that possible replies may not
 * be received in full. To support this type of usage, the sending and
 * receiving of data on the same side (e.g. on the [[Client]]) need to be
 * coordinated such that it is known when all replies have been received.
 * Only then should the transport be shut down.
 *
 * To support these scenarios it is recommended that the full-duplex
 * transport integration is configurable in terms of termination handling,
 * which means that the user can optionally suppress the normal (closing)
 * reaction to completion or cancellation events, as is expressed by the
 * possible values of this type:
 *
 *  - [[EagerClose]] means to not ignore signals
 *  - [[IgnoreCancel]] means to not react to cancellation of the receiving
 *    side unless the sending side has already completed
 *  - [[IgnoreComplete]] means to not react to the completion of the sending
 *    side unless the receiving side has already canceled
 *  - [[IgnoreBoth]] means to ignore the first termination signal—be that
 *    cancellation or completion—and only act upon the second one
 */
sealed abstract class TLSClosing {
  def ignoreCancel: Boolean
  def ignoreComplete: Boolean
}
object TLSClosing {

  /**
   * Java API: obtain the [[EagerClose]] singleton value.
   */
  def eagerClose: TLSClosing = EagerClose

  /**
   * Java API: obtain the [[IgnoreCancel]] singleton value.
   */
  def ignoreCancel: TLSClosing = IgnoreCancel

  /**
   * Java API: obtain the [[IgnoreComplete]] singleton value.
   */
  def ignoreComplete: TLSClosing = IgnoreComplete

  /**
   * Java API: obtain the [[IgnoreBoth]] singleton value.
   */
  def ignoreBoth: TLSClosing = IgnoreBoth
}

/**
 * see [[TLSClosing]]
 */
sealed abstract class EagerClose extends TLSClosing {
  override def ignoreCancel = false
  override def ignoreComplete = false
}
case object EagerClose extends EagerClose

/**
 * see [[TLSClosing]]
 */
sealed abstract class IgnoreCancel extends TLSClosing {
  override def ignoreCancel = true
  override def ignoreComplete = false
}
case object IgnoreCancel extends IgnoreCancel

/**
 * see [[TLSClosing]]
 */
sealed abstract class IgnoreComplete extends TLSClosing {
  override def ignoreCancel = false
  override def ignoreComplete = true
}
case object IgnoreComplete extends IgnoreComplete

/**
 * see [[TLSClosing]]
 */
sealed abstract class IgnoreBoth extends TLSClosing {
  override def ignoreCancel = true
  override def ignoreComplete = true
}
case object IgnoreBoth extends IgnoreBoth

object TLSProtocol {

  /**
   * This is the supertype of all messages that the SslTls operator emits on the
   * plaintext side.
   */
  sealed trait SslTlsInbound

  /**
   * If the underlying transport is closed before the final TLS closure command
   * is received from the peer then the SSLEngine will throw an SSLException that
   * warns about possible truncation attacks. This exception is caught and
   * translated into this message when encountered. Most of the time this occurs
   * not because of a malicious attacker but due to a connection abort or a
   * misbehaving communication peer.
   */
  sealed abstract class SessionTruncated extends SslTlsInbound

  case object SessionTruncated extends SessionTruncated

  /**
   * Plaintext bytes emitted by the SSLEngine are received over one specific
   * encryption session and this class bundles the bytes with the SSLSession
   * object. When the session changes due to renegotiation (which can be
   * initiated by either party) the new session value will not compare equal to
   * the previous one.
   *
   * The Java API for getting session information is given by the SSLSession object,
   * the Scala API adapters are offered below.
   */
  final case class SessionBytes(session: SSLSession, bytes: ByteString)
      extends SslTlsInbound
      with scaladsl.ScalaSessionAPI

  /**
   * This is the supertype of all messages that the SslTls operator accepts on its
   * plaintext side.
   */
  sealed trait SslTlsOutbound

  /**
   * Initiate a new session negotiation. Any [[SendBytes]] commands following
   * this one will be held back (i.e. back-pressured) until the new handshake is
   * completed, meaning that the bytes following this message will be encrypted
   * according to the requirements outlined here.
   *
   * Each of the values in this message is optional and will have the following
   * effect if provided:
   *
   * - `enabledCipherSuites` will be passed to `SSLEngine::setEnabledCipherSuites()`
   * - `enabledProtocols` will be passed to `SSLEngine::setEnabledProtocols()`
   * - `clientAuth` will be passed to `SSLEngine::setWantClientAuth()` or `SSLEngine.setNeedClientAuth()`, respectively
   * - `sslParameters` will be passed to `SSLEngine::setSSLParameters()`
   *
   * Please note that passing `clientAuth = None` means that no change is done
   * on client authentication requirements while `clientAuth = Some(ClientAuth.None)`
   * switches off client authentication.
   */
  case class NegotiateNewSession(
      enabledCipherSuites: Option[immutable.Seq[String]],
      enabledProtocols: Option[immutable.Seq[String]],
      clientAuth: Option[TLSClientAuth],
      sslParameters: Option[SSLParameters])
      extends SslTlsOutbound {

    /**
     * Java API: Make a copy of this message with the given `enabledCipherSuites`.
     */
    @varargs
    def withCipherSuites(s: String*): NegotiateNewSession = copy(enabledCipherSuites = Some(s.toList))

    /**
     * Java API: Make a copy of this message with the given `enabledProtocols`.
     */
    @varargs
    def withProtocols(p: String*): NegotiateNewSession = copy(enabledProtocols = Some(p.toList))

    /**
     * Java API: Make a copy of this message with the given [[TLSClientAuth]] setting.
     */
    def withClientAuth(ca: TLSClientAuth): NegotiateNewSession = copy(clientAuth = Some(ca))

    /**
     * Java API: Make a copy of this message with the given [[SSLParameters]].
     */
    def withParameters(p: SSLParameters): NegotiateNewSession = copy(sslParameters = Some(p))
  }

  object NegotiateNewSession extends NegotiateNewSession(None, None, None, None) {

    /**
     * Java API: obtain the default value (which will leave the SSLEngine’s
     * settings unchanged).
     */
    def withDefaults: NegotiateNewSession = this

  }

  /**
   * Java API: obtain the default value of [[NegotiateNewSession]] (which will leave the SSLEngine’s
   * settings unchanged).
   */
  def negotiateNewSession: NegotiateNewSession = NegotiateNewSession

  /**
   * Send the given [[akka.util.ByteString]] across the encrypted session to the
   * peer.
   */
  final case class SendBytes(bytes: ByteString) extends SslTlsOutbound

}

/**
 * An SSLEngine can either demand, allow or ignore its peer’s authentication
 * (via certificates), where `Need` will fail the handshake if the peer does
 * not provide valid credentials, `Want` allows the peer to send credentials
 * and verifies them if provided, and `None` disables peer certificate
 * verification.
 *
 * See the documentation for `SSLEngine::setWantClientAuth` for more information.
 */
sealed abstract class TLSClientAuth
object TLSClientAuth {
  case object None extends TLSClientAuth
  case object Want extends TLSClientAuth
  case object Need extends TLSClientAuth

  def none: TLSClientAuth = None
  def want: TLSClientAuth = Want
  def need: TLSClientAuth = Need
}
