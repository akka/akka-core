/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509KeyManager
import javax.net.ssl.X509TrustManager

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Guards the certificate-chain selection in [[PemManagersProvider.buildKeyManagers]]: the
 * presented chain must contain only the leaf and the CA that actually issued it, not every
 * CA from the `ca-cert-file` bundle. A peer validating that chain with a TrustManager that
 * does not do full path-building (e.g. "SunX509") would reject it if an unrelated or
 * expired CA were placed ahead of the real issuer.
 */
class PemManagersProviderChainSpec extends AnyWordSpec with Matchers {

  private def nameToPath(name: String): String = getClass.getClassLoader.getResource(name).getPath

  private def loadCert(name: String): X509Certificate =
    PemManagersProvider.loadCertificate(nameToPath(name)).asInstanceOf[X509Certificate]

  private val privateKey = PemManagersProvider.loadPrivateKey(nameToPath("ssl/node.example.com.pem"))
  private val leafCert = loadCert("ssl/node.example.com.crt")
  private val realCa = loadCert("ssl/exampleca.crt")
  // Unrelated to the leaf cert's issuance chain, and expired -- stands in for an old CA
  // that a rotation bundle still carries during the overlap window.
  private val otherCa = loadCert("ssl/pem/selfsigned-certificate.pem")

  private def presentedChain(key: PrivateKey, leaf: X509Certificate)(
      cacerts: Seq[X509Certificate]): Array[X509Certificate] = {
    val keyManagers = PemManagersProvider.buildKeyManagers(key, leaf, cacerts)
    val km = keyManagers.collectFirst { case k: X509KeyManager => k }.get
    km.getCertificateChain("private-key")
  }

  private def checkServerTrusted(trustAnchor: X509Certificate, chain: Array[X509Certificate]): Unit = {
    // SunX509 does not do CertPathBuilder-style path building the way the default (PKIX)
    // TrustManager does; it validates the presented chain close to as-is. That's why an
    // unrelated/expired CA ahead of the real issuer trips it up while PKIX tolerates it -
    // using it explicitly here is what makes the scenario observable.
    val trustStore = KeyStore.getInstance("JKS")
    trustStore.load(null)
    trustStore.setCertificateEntry("anchor", trustAnchor)
    val tmf = TrustManagerFactory.getInstance("SunX509")
    tmf.init(trustStore)
    val tm = tmf.getTrustManagers.collectFirst { case t: X509TrustManager => t }.get
    tm.checkServerTrusted(chain, "RSA")
  }

  "PemManagersProvider.buildKeyManagers" must {

    "present only the real issuer in the certificate chain, not every CA in the bundle" in {
      // Bundle order mirrors a rotation file: old (unrelated/expired) CA first, real CA second.
      val chain = presentedChain(privateKey, leafCert)(Seq(otherCa, realCa))

      chain.length must be(2)
      chain(1) must be(realCa)
      // A peer validating this chain with a non-path-building TrustManager must still succeed.
      noException must be thrownBy checkServerTrusted(realCa, chain)
    }

    "pick the CA that actually signed the leaf when two bundle CAs share the issuer DN" in {
      // cert-manager renewing a CA in place keeps the subject DN and changes only the key.
      val caOld = loadCert("ssl/rotation-same-dn/ca-old.crt")
      val caNew = loadCert("ssl/rotation-same-dn/ca-new.crt")
      val node = loadCert("ssl/rotation-same-dn/node.crt")
      val nodeKey = PemManagersProvider.loadPrivateKey(nameToPath("ssl/rotation-same-dn/node.pem"))
      caOld.getSubjectX500Principal must be(caNew.getSubjectX500Principal)

      // Old CA first: a subject-DN-only match would wrongly pick it.
      val chain = presentedChain(nodeKey, node)(Seq(caOld, caNew))

      chain.length must be(2)
      chain(1) must be(caNew)
      noException must be thrownBy checkServerTrusted(caNew, chain)
    }

    "present the leaf alone when no CA in the bundle issued it" in {
      val chain = presentedChain(privateKey, leafCert)(Seq(otherCa))

      chain.length must be(1)
      chain(0) must be(leafCert)
    }
  }

  "PemManagersProvider.findIssuer" must {
    "return None when only a same-DN non-issuing CA is present" in {
      val caOld = loadCert("ssl/rotation-same-dn/ca-old.crt")
      val node = loadCert("ssl/rotation-same-dn/node.crt")
      PemManagersProvider.findIssuer(node, Seq(caOld)) must be(None)
    }
  }
}
