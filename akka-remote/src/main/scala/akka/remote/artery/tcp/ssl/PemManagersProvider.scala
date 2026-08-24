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
import java.util.Collections
import java.util.{ Collection => JCollection }
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
      cacert: Certificate): Array[KeyManager] =
    buildKeyManagers(privateKey, cert, Collections.singletonList(cacert))

  /**
   * INTERNAL API
   */
  @InternalApi
  private[ssl] def buildKeyManagers(
      privateKey: PrivateKey,
      cert: X509Certificate,
      cacerts: JCollection[_ <: Certificate]): Array[KeyManager] = {
    val keyStore = KeyStore.getInstance("JKS")
    keyStore.load(null)

    keyStore.setCertificateEntry("cert", cert)
    val cacertArray: Array[Certificate] = cacerts.asScala.toArray[Certificate]
    cacertArray.zipWithIndex.foreach {
      case (ca, i) => keyStore.setCertificateEntry(s"cacert-$i", ca)
    }
    val chain: Array[Certificate] = cert +: cacertArray
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
  private[ssl] def buildTrustManagers(cacert: Certificate): Array[TrustManager] =
    buildTrustManagers(Collections.singletonList(cacert))

  /**
   * INTERNAL API
   */
  @InternalApi
  private[ssl] def buildTrustManagers(cacerts: JCollection[_ <: Certificate]): Array[TrustManager] = {
    val trustStore = KeyStore.getInstance("JKS")
    trustStore.load(null)
    cacerts.asScala.zipWithIndex.foreach {
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
   * Loads every PEM-encoded certificate from `filename`. Unlike [[loadCertificate]] (which
   * calls the JDK's singular `generateCertificate` and silently ignores all but the first
   * entry), this reads a full CA bundle — required for scenarios such as CA rotation where
   * a trust file legitimately contains multiple certificates.
   */
  @InternalApi
  private[ssl] def loadCertificates(filename: String): JCollection[_ <: Certificate] = blocking {
    val bytes = Files.readAllBytes(new File(filename).toPath)
    certFactory.generateCertificates(new ByteArrayInputStream(bytes))
  }

}
