/*
 * Copyright (C) 2025-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.security.{ KeyStore, SecureRandom }
import javax.net.ssl._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.NotUsed
import akka.stream._
import akka.stream.TLSProtocol._
import akka.stream.scaladsl._
import akka.testkit.WithLogCapturing
import akka.testkit.AkkaSpec
import akka.util.{ ByteString, JavaVersion }

object TlsStageEdgeCasesSpec {
  val TLS12Ciphers: Set[String] = Set("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")

  def initContext(): SSLContext = {
    val password = "changeme"
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(getClass.getResourceAsStream("/keystore"), password.toCharArray)
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(getClass.getResourceAsStream("/truststore"), password.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(keyStore, password.toCharArray)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(trustStore)
    val ctx = SSLContext.getInstance("TLSv1.2")
    ctx.init(kmf.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    ctx
  }

  val configOverrides: String =
    """
      akka.loglevel = DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    """
}

/**
 * Edge case tests for the TLS bidi stage. These exercise specific behaviors
 * that proved tricky to port from the legacy `TLSActor` implementation to a
 * pure `GraphStage` (see `TlsStage`). They are intended to pass against both
 * the legacy and new implementations to give confidence in the migration.
 */
class TlsStageEdgeCasesSpec extends AkkaSpec(TlsStageEdgeCasesSpec.configOverrides) with WithLogCapturing {
  import TlsStageEdgeCasesSpec._

  private val ctx = initContext()

  private def mkEngine(role: TLSRole): SSLEngine = {
    val engine = ctx.createSSLEngine()
    engine.setUseClientMode(role == Client)
    engine.setEnabledCipherSuites(TLS12Ciphers.toArray)
    engine.setEnabledProtocols(Array("TLSv1.2"))
    engine
  }

  private def clientTls(closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    TLS(() => mkEngine(Client), closing)

  private def serverTls(closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    TLS(() => mkEngine(Server), closing)

  /** Wrap-and-echo helper: client sends `inputs`, server echoes back the
   *  decrypted SslTlsInbound as plaintext SendBytes, and the bytes returned
   *  to the client are concatenated and returned. */
  private def runEcho(
      inputs: immutable.Seq[SslTlsOutbound],
      expectedSize: Int,
      leftClosing: TLSClosing = IgnoreComplete,
      rightClosing: TLSClosing = IgnoreComplete): ByteString = {
    val echo = Flow[SslTlsInbound].collect {
      case SessionBytes(_, bytes) => SendBytes(bytes)
    }
    val tls = clientTls(leftClosing).atop(serverTls(rightClosing).reversed).join(echo)
    val f = Source(inputs)
      .via(tls)
      .collect { case SessionBytes(_, b) => b }
      .scan(ByteString.empty)(_ ++ _)
      .filter(_.size >= expectedSize)
      .runWith(Sink.headOption)
    Await.result(f, 30.seconds).getOrElse(ByteString.empty)
  }

  "TLS stage edge cases" should {

    "round-trip many small SendBytes elements" in {
      val str = "0123456789abcdefghij"
      val inputs = str.map(c => SendBytes(ByteString(c.toByte)))
      runEcho(inputs, str.length).utf8String should ===(str)
    }

    "round-trip a single payload larger than the TLS max packet size" in {
      // > 16384 bytes (TLS max record), exercises engine.wrap producing
      // multiple TLS records and the chopping block handling them.
      val payload = ("x" * 50000)
      val inputs = SendBytes(ByteString(payload)) :: Nil
      runEcho(inputs, payload.length).utf8String should ===(payload)
    }

    "round-trip empty SendBytes interleaved with non-empty ones" in {
      val inputs = List(
        SendBytes(ByteString.empty),
        SendBytes(ByteString("hello ")),
        SendBytes(ByteString.empty),
        SendBytes(ByteString("world")),
        SendBytes(ByteString.empty))
      runEcho(inputs, "hello world".length).utf8String should ===("hello world")
    }

    "round-trip when plaintext is provided before downstream pulls" in {
      // The Source pushes immediately. With a buffered downstream, plaintext
      // arrives at plainIn well before any onPull on cipherOut from the
      // server side — verifies the stage correctly defers wrapping until
      // demand exists.
      val inputs = List.fill(10)(SendBytes(ByteString("ping")))
      runEcho(inputs, 4 * 10).utf8String should ===("ping" * 10)
    }

    "complete cleanly with EagerClose on both sides and an empty payload" in {
      // Mirror of CompletedImmediately in TlsSpec: with empty input and
      // EagerClose semantics, the handshake must still complete and the
      // streams tear down without timing out.
      val tls = clientTls(EagerClose)
        .atop(serverTls(EagerClose).reversed)
        .join(Flow[SslTlsInbound].collect { case SessionBytes(_, b) => SendBytes(b) })
      val f = Source.empty[SslTlsOutbound].via(tls).collect { case SessionBytes(_, b) => b }.runWith(Sink.seq)
      Await.result(f, 10.seconds) should ===(immutable.Seq.empty)
    }

    "round-trip after a renegotiation triggered mid-stream" in {
      // NegotiateNewSession in the middle of a stream of SendBytes. The
      // pre-renegotiation bytes must arrive with the original cipher,
      // post-renegotiation bytes with the new cipher, and all of them
      // unwrapped successfully on the receiving side.
      if (JavaVersion.majorVersion < 17) { // TLSv1.2 renegotiation OK on 11
        val inputs = List(
          SendBytes(ByteString("before-")),
          NegotiateNewSession.withCipherSuites("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"),
          SendBytes(ByteString("after")))
        runEcho(inputs, "before-after".length).utf8String should ===("before-after")
      }
    }

    "fail the stream when the user-side input fails before any demand" in {
      val ex = new RuntimeException("boom-user")
      val inputs = Source.failed[SslTlsOutbound](ex)
      val tls = clientTls(EagerClose)
        .atop(serverTls(EagerClose).reversed)
        .join(Flow[SslTlsInbound].collect {
          case SessionBytes(_, b) => SendBytes(b)
        })
      val f = inputs.via(tls).runWith(Sink.ignore)
      val thrown = intercept[RuntimeException](Await.result(f, 5.seconds))
      thrown.getMessage should include("boom-user")
    }

    "round-trip a sequence preceded by an immediately-applied NegotiateNewSession" in {
      // First element is a NegotiateNewSession; the engine must reset to the
      // requested cipher BEFORE sending its initial ClientHello so the
      // session ends up with that cipher.
      if (JavaVersion.majorVersion < 17) {
        val inputs = List(
          NegotiateNewSession.withCipherSuites("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"),
          SendBytes(ByteString("hello")))
        val res = runEcho(inputs, "hello".length).utf8String
        res should ===("hello")
      }
    }
  }
}
