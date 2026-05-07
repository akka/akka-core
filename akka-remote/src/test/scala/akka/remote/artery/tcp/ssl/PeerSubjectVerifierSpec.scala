/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.Principal
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSessionContext

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PeerSubjectVerifierSpec extends AnyWordSpec with Matchers {
  import TlsResourcesSpec._
  "PeerSubjectVerifier" must {

    "accept a peer when both peers have the same subject name" in {
      // CN=one.example.com
      // SAN=DNS:number-one.example.com,DNS:example.com
      // see https://github.com/playframework/play-samples/pull/97
      val exampleOne = loadCert("/ssl/one.example.com.crt")
      // CN=two.example.com
      // SAN=DNS:number-two.example.com,DNS:example.com
      val exampleTwo = loadCert("/ssl/two.example.com.crt")
      // verification passes because both certs have `example.com` in the SAN
      new PeerSubjectVerifier(exampleOne).verifyServerSession(null, inMemSession(exampleTwo)) mustBe None
    }

    "reject a peer when peers don't have the same subject name" in {
      // `island.example.com` has no SAN and its only subject is not available on `two.example.com`
      // the peer verification must fail.
      val client = loadCert("/ssl/island.example.com.crt")
      val exampleTwo = loadCert("/ssl/two.example.com.crt")
      new PeerSubjectVerifier(client).verifyServerSession(null, inMemSession(exampleTwo)) mustNot be(None)
    }
  }

  def inMemSession(peerCert: X509Certificate): SSLSession = {
    new SSLSession {
      override def getPeerCertificates: Array[Certificate] = Array(peerCert)
      override def getId: Array[Byte] = throw new UnsupportedOperationException()
      override def getSessionContext: SSLSessionContext = throw new UnsupportedOperationException()
      override def getCreationTime: Long = throw new UnsupportedOperationException()
      override def getLastAccessedTime: Long = throw new UnsupportedOperationException()
      override def invalidate(): Unit = throw new UnsupportedOperationException()
      override def isValid: Boolean = throw new UnsupportedOperationException()
      override def putValue(name: String, value: Any): Unit = throw new UnsupportedOperationException()
      override def getValue(name: String): AnyRef = throw new UnsupportedOperationException()
      override def removeValue(name: String): Unit = throw new UnsupportedOperationException()
      override def getValueNames: Array[String] = throw new UnsupportedOperationException()
      override def getLocalCertificates: Array[Certificate] = throw new UnsupportedOperationException()
      override def getPeerCertificateChain = throw new UnsupportedOperationException()
      override def getPeerPrincipal: Principal = throw new UnsupportedOperationException()
      override def getLocalPrincipal: Principal = throw new UnsupportedOperationException()
      override def getCipherSuite: String = throw new UnsupportedOperationException()
      override def getProtocol: String = throw new UnsupportedOperationException()
      override def getPeerHost: String = throw new UnsupportedOperationException()
      override def getPeerPort: Int = throw new UnsupportedOperationException()
      override def getPacketBufferSize: Int = throw new UnsupportedOperationException()
      override def getApplicationBufferSize: Int = throw new UnsupportedOperationException()
    }
  }
}
