/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.io.ByteArrayInputStream
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory

import scala.concurrent.blocking
import scala.jdk.CollectionConverters._
import scala.util.Try

import akka.annotation.InternalApi
import akka.pki.pem.DERPrivateKeyLoader
import akka.pki.pem.PEMDecoder

/**
 * INTERNAL API
 */
@InternalApi
private[ssl] object PemManagersProvider {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[ssl] def buildKeyManagers(
      privateKey: PrivateKey,
      cert: X509Certificate,
      issuer: Option[X509Certificate]): Array[KeyManager] = {
    val keyStore = KeyStore.getInstance("JKS")
    keyStore.load(null)

    keyStore.setCertificateEntry("cert", cert)
    // Present only the leaf and its issuer, never the rest of a rotation bundle: a
    // non-path-building TrustManager (e.g. SunX509) rejects an unrelated CA ahead of it.
    val chain: Array[Certificate] = issuer match {
      case Some(ca) =>
        keyStore.setCertificateEntry("cacert", ca)
        Array(cert, ca)
      case None => Array(cert)
    }
    keyStore.setKeyEntry("private-key", privateKey, "changeit".toCharArray, chain)

    val kmf =
      KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(keyStore, "changeit".toCharArray)
    val keyManagers = kmf.getKeyManagers
    keyManagers
  }

  /**
   * INTERNAL API
   *
   * The CA certificate from `cacerts` whose key actually signed `cert`, if present.
   * Matching on the issuer/subject DN alone is not enough: a same-DN CA rotation (the CA
   * is renewed in place, keeping its DN and changing only its key) leaves the bundle
   * holding two CAs with the same subject DN, and only one of them is the real issuer.
   */
  @InternalApi
  private[ssl] def findIssuer(cert: X509Certificate, cacerts: Seq[X509Certificate]): Option[X509Certificate] =
    cacerts.iterator
      .filter(_.getSubjectX500Principal == cert.getIssuerX500Principal)
      .find(ca => Try(cert.verify(ca.getPublicKey)).isSuccess)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[ssl] def buildTrustManagers(cacerts: Seq[Certificate]): Array[TrustManager] = {
    // TrustManagerFactory.init on an empty keystore silently yields a trust-nothing
    // TrustManager instead of failing, so an empty cacerts must be rejected here rather
    // than left to whichever caller happens to guard against it.
    require(cacerts.nonEmpty, "cacerts must not be empty")
    val trustStore = KeyStore.getInstance("JKS")
    trustStore.load(null)
    cacerts.zipWithIndex.foreach {
      case (ca, i) => trustStore.setCertificateEntry(s"cacert-$i", ca)
    }

    val tmf =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(trustStore)
    tmf.getTrustManagers
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[ssl] def loadPrivateKey(filename: String): PrivateKey = blocking {
    val bytes = Files.readAllBytes(new File(filename).toPath)
    val pemData = new String(bytes, Charset.forName("UTF-8"))
    DERPrivateKeyLoader.load(PEMDecoder.decode(pemData))
  }

  private val certFactory = CertificateFactory.getInstance("X.509")

  /**
   * INTERNAL API
   */
  @InternalApi
  private[ssl] def loadCertificate(filename: String): Certificate = blocking {
    val bytes = Files.readAllBytes(new File(filename).toPath)
    certFactory.generateCertificate(new ByteArrayInputStream(bytes))
  }

  /**
   * INTERNAL API
   *
   * Loads every PEM-encoded certificate from `filename`. Use this for a CA trust file,
   * which may legitimately contain more than one certificate, for example during a CA
   * rotation.
   */
  @InternalApi
  private[ssl] def loadCertificates(filename: String): Seq[X509Certificate] = blocking {
    val bytes = Files.readAllBytes(new File(filename).toPath)
    // CertificateFactory.getInstance("X.509") only ever produces X509Certificate instances.
    certFactory
      .generateCertificates(new ByteArrayInputStream(bytes))
      .asScala
      .iterator
      .map(_.asInstanceOf[X509Certificate])
      .toVector
  }

}
