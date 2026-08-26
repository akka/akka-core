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
      cacerts: Seq[Certificate]): Array[KeyManager] = {
    val keyStore = KeyStore.getInstance("JKS")
    keyStore.load(null)

    keyStore.setCertificateEntry("cert", cert)
    // Only the CA that actually issued `cert` belongs in its chain. The other CAs in the
    // bundle (e.g. an old CA still present during a rotation overlap window) are trust
    // anchors, not issuers, and must not be presented as part of this chain: a peer
    // validating with a TrustManager that doesn't build alternate paths (e.g. SunX509)
    // will reject the chain if one of those unrelated CAs is invalid, even though it is
    // not the actual issuer.
    val issuer = cacerts.collectFirst {
      case ca: X509Certificate if ca.getSubjectX500Principal == cert.getIssuerX500Principal => ca
    }
    val chain: Array[Certificate] = issuer match {
      case Some(ca) => Array(cert, ca)
      case None     => (cert +: cacerts).toArray
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
   */
  @InternalApi
  private[ssl] def buildTrustManagers(cacerts: Seq[Certificate]): Array[TrustManager] = {
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
   * Loads every PEM-encoded certificate from `filename`, in file order. Use this for a
   * CA trust file, which may legitimately contain more than one certificate, for example
   * during a CA rotation.
   */
  @InternalApi
  private[ssl] def loadCertificates(filename: String): Seq[Certificate] = blocking {
    val bytes = Files.readAllBytes(new File(filename).toPath)
    certFactory.generateCertificates(new ByteArrayInputStream(bytes)).asScala.toVector
  }

}
