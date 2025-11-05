/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession

import akka.annotation.InternalApi

/**
 * Allows hooking in extra verification before finishing the SSL handshake.
 *
 * INTERNAL API
 */
@InternalApi
private[ssl] trait SessionVerifier {
  def verifyClientSession(hostname: String, session: SSLSession): Option[Throwable]
  def verifyServerSession(hostname: String, session: SSLSession): Option[Throwable]
}

/**
 * This verifier approves all sessions.
 *
 * INTERNAL API
 */
@InternalApi
private[ssl] object NoopSessionVerifier extends SessionVerifier {
  override def verifyClientSession(hostname: String, session: SSLSession): Option[Throwable] = None
  override def verifyServerSession(hostname: String, session: SSLSession): Option[Throwable] = None
}

/**
 * This is a TLS session verifier that checks the peer has a subject name that matches
 * the subject name of the given certificate. This can be useful to prevent accidentally
 * connecting with other nodes that have certificates that, while being signed by the
 * same certificate authority, belong to different clusters.
 *
 * INTERNAL API
 */
@InternalApi
private[ssl] final class PeerSubjectVerifier(peerCertificate: X509Certificate) extends SessionVerifier {
  override def verifyClientSession(hostname: String, session: SSLSession): Option[Throwable] =
    verifyPeerCertificates(session)
  override def verifyServerSession(hostname: String, session: SSLSession): Option[Throwable] =
    verifyPeerCertificates(session)

  private def verifyPeerCertificates(session: SSLSession) = {
    val mySubjectNames = X509Readers.getAllSubjectNames(peerCertificate)
    if (session.getPeerCertificates.length == 0) {
      Some(new IllegalArgumentException("No peer certificates"))
    }
    session.getPeerCertificates()(0) match {
      case x509: X509Certificate =>
        val peerSubjectNames =
          X509Readers.getAllSubjectNames(x509)
        if (mySubjectNames.exists(peerSubjectNames)) None
        else
          Some(
            new IllegalArgumentException(
              s"None of the peer subject names $peerSubjectNames were in local subject names $mySubjectNames"))
      case other =>
        Some(new IllegalArgumentException(s"Unknown certificate type: ${other.getClass}"))
    }
  }

}
