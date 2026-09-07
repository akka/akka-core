/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.PrivateKey
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import javax.net.ssl.X509TrustManager

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 *
 */
class PemManagersProviderSpec extends AnyWordSpec with Matchers {

  "A PemManagersProvider" must {

    "load stores reading files setup in config (akka-pki samples)" in {
      // These set of certificates are valid PEMs but are invalid for akka-remote
      // use. Either the key length, certificate usage limitations (via the UsageKeyExtensions),
      // or the fact that the key's certificate is self-signed cause one of the following
      // errors: `certificate_unknown`, `certificate verify message signature error`/`bad_certificate`
      // during the SSLHandshake.
      withFiles("ssl/pem/pkcs1.pem", "ssl/pem/selfsigned-certificate.pem", "ssl/pem/selfsigned-certificate.pem") {
        (pk, cert, cacert) =>
          PemManagersProvider.buildKeyManagers(pk, cert, Some(cacert.asInstanceOf[X509Certificate])).length must be(1)
          PemManagersProvider.buildTrustManagers(Seq(cacert)).length must be(1)
          cert.getSubjectDN.getName must be("CN=0d207b68-9a20-4ee8-92cb-bf9699581cf8")
      }
    }

    "load stores reading files setup in config (keytool samples)" in {
      withFiles("ssl/node.example.com.pem", "ssl/node.example.com.crt", "ssl/exampleca.crt") { (pk, cert, cacert) =>
        PemManagersProvider.buildKeyManagers(pk, cert, Some(cacert.asInstanceOf[X509Certificate])).length must be(1)
        PemManagersProvider.buildTrustManagers(Seq(cacert)).length must be(1)
        cert.getSubjectDN.getName must be(
          "CN=node.example.com, OU=Example Org, O=Example Company, L=San Francisco, ST=California, C=US")
      }
    }

    "load every CA from a multi-cert PEM bundle" in {
      // ssl/rotation-ca2/ca-bundle.crt is exampleca.crt and rotation-ca2/exampleca2.crt
      // concatenated -- two independent, valid, non-expired root CAs, mirroring a CA
      // rotation bundle that must trust both an old and a new CA for an overlap window.
      // Loading it here also guards that the committed bundle stays valid.
      val cacerts = PemManagersProvider.loadCertificates(nameToPath("ssl/rotation-ca2/ca-bundle.crt"))
      cacerts.size must be(2)

      val trustManagers = PemManagersProvider.buildTrustManagers(cacerts)
      val anchors = trustManagers.collect {
        case tm: X509TrustManager => tm.getAcceptedIssuers.toList
      }.flatten
      anchors.size must be(2)
      anchors.toSet must be(cacerts.toSet)
    }

  }

  private def withFiles(keyFile: String, certFile: String, caCertFile: String)(
      block: (PrivateKey, X509Certificate, Certificate) => Unit) = {
    block(
      PemManagersProvider.loadPrivateKey(nameToPath(keyFile)),
      PemManagersProvider.loadCertificate(nameToPath(certFile)).asInstanceOf[X509Certificate],
      PemManagersProvider.loadCertificate(nameToPath(caCertFile)))
  }

  private def nameToPath(name: String): String = getClass.getClassLoader.getResource(name).getPath
}
