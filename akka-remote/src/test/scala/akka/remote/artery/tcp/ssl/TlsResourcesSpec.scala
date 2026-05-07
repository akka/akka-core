/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate

import scala.util.Try
import scala.util.control.NonFatal

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

/**
 *
 */
class TlsResourcesSpec extends AnyWordSpec with Matchers {

  import TlsResourcesSpec._

  val baseNode = "node"
  val baseClient = "client"
  val baseRsaClient = "rsa-client"
  val baseIslandServer = "island"
  val baseServers = Set("one", "two")
  val arteryNodeSet = Set("artery-nodes/artery-node001", "artery-nodes/artery-node002", "artery-nodes/artery-node003")

  "example.com certificate family" must {
    "all include `example.com` as SAN" in {
      val sameSan = baseServers + baseClient + baseNode + baseRsaClient
      sameSan.foreach { prefix =>
        val serverCert = loadCert(s"/ssl/$prefix.example.com.crt")
        X509Readers.getAllSubjectNames(serverCert).contains("example.com") mustBe (true)
      }
    }

    "not add `example.com` as SAN on the `island.example.com` certificate" in {
      val notExampleSan = arteryNodeSet + baseIslandServer
      notExampleSan.foreach { prefix =>
        val cert = loadCert(s"/ssl/$prefix.example.com.crt")
        X509Readers.getAllSubjectNames(cert).contains("example.com") mustBe (false)
      }
    }

    val serverAuth = "1.3.6.1.5.5.7.3.1" // TLS Web server authentication
    val clientAuth = "1.3.6.1.5.5.7.3.2" // TLS Web client authentication
    "have certificates usable for client Auth" in {
      val clients = Set(baseClient, baseNode, baseRsaClient) ++ arteryNodeSet
      clients.foreach { prefix =>
        val cert = loadCert(s"/ssl/$prefix.example.com.crt")
        cert.getExtendedKeyUsage.asScala.contains(clientAuth) mustBe (true)
      }
    }

    "have certificates usable for server Auth" in {
      val servers = baseServers + baseIslandServer + baseNode ++ arteryNodeSet
      servers.foreach { prefix =>
        val serverCert = loadCert(s"/ssl/$prefix.example.com.crt")
        serverCert.getExtendedKeyUsage.asScala.contains(serverAuth) mustBe (true)
      }
    }

    "have RSA Private Keys in PEM format" in {
      val nodes = arteryNodeSet + baseNode + baseRsaClient
      nodes.foreach { prefix =>
        try {
          val pk = PemManagersProvider.loadPrivateKey(toAbsolutePath(s"ssl/$prefix.example.com.pem"))
          pk.getAlgorithm must be("RSA")
        } catch {
          case NonFatal(t) => fail(s"Failed test for $prefix", t)
        }
      }
    }

  }

}

object TlsResourcesSpec {

  def toAbsolutePath(resourceName: String): String =
    getClass.getClassLoader.getResource(resourceName).getPath

  private val certFactory = CertificateFactory.getInstance("X.509")
  def loadCert(resourceName: String): X509Certificate = {
    val fin = classOf[X509ReadersSpec].getResourceAsStream(resourceName)
    try certFactory.generateCertificate(fin).asInstanceOf[X509Certificate]
    finally Try(fin.close())
  }
}
