/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.cert.X509Certificate
import java.util.Arrays
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509KeyManager
import javax.net.ssl.X509TrustManager

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Reproduces the "wrong certificate chain" issue: [[PemManagersProvider.buildKeyManagers]]
 * puts every CA from the `ca-cert-file` bundle into the presented chain, not just the one
 * that actually issued the leaf certificate. When a peer validates that chain with a
 * TrustManager that doesn't do full path-building (e.g. "SunX509"), an unrelated/expired CA
 * placed ahead of the real issuer breaks the handshake.
 */
class PemManagersProviderChainSpec extends AnyWordSpec with Matchers {

  private def nameToPath(name: String): String = getClass.getClassLoader.getResource(name).getPath

  private val privateKey = PemManagersProvider.loadPrivateKey(nameToPath("ssl/node.example.com.pem"))
  private val leafCert =
    PemManagersProvider.loadCertificate(nameToPath("ssl/node.example.com.crt")).asInstanceOf[X509Certificate]
  private val realCa =
    PemManagersProvider.loadCertificate(nameToPath("ssl/exampleca.crt")).asInstanceOf[X509Certificate]
  // Unrelated to the leaf cert's issuance chain, and expired -- stands in for an old CA
  // that a rotation bundle still carries during the overlap window.
  private val otherCa =
    PemManagersProvider.loadCertificate(nameToPath("ssl/pem/selfsigned-certificate.pem")).asInstanceOf[X509Certificate]

  private def presentedChain(cacerts: java.util.List[X509Certificate]): Array[X509Certificate] = {
    val keyManagers = PemManagersProvider.buildKeyManagers(privateKey, leafCert, cacerts)
    val km = keyManagers.collectFirst { case k: X509KeyManager => k }.get
    km.getCertificateChain("private-key")
  }

  private def checkServerTrusted(chain: Array[X509Certificate]): Unit = {
    // SunX509 does not do CertPathBuilder-style path building the way the default (PKIX)
    // TrustManager does; it validates the presented chain close to as-is. That's why an
    // unrelated/expired CA ahead of the real issuer trips it up while PKIX tolerates it -
    // using it explicitly here is what makes the bug reproducible.
    val trustStore = java.security.KeyStore.getInstance("JKS")
    trustStore.load(null)
    trustStore.setCertificateEntry("realca", realCa)
    val tmf = TrustManagerFactory.getInstance("SunX509")
    tmf.init(trustStore)
    val tm = tmf.getTrustManagers.collectFirst { case t: X509TrustManager => t }.get
    tm.checkServerTrusted(chain, "RSA")
  }

  "PemManagersProvider.buildKeyManagers" must {

    "present only the real issuer in the certificate chain, not every CA in the bundle" in {
      // Bundle order mirrors a rotation file: old (unrelated/expired) CA first, real CA second.
      val chain = presentedChain(Arrays.asList(otherCa, realCa))

      // A peer validating this chain with a non-path-building TrustManager must still
      // succeed: the only CA in the chain besides the leaf should be the real issuer.
      noException must be thrownBy checkServerTrusted(chain)
    }
  }
}
