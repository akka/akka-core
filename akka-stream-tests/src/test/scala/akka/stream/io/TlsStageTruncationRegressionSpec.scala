/*
 * Copyright (C) 2025-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import javax.net.ssl.SSLEngine

import scala.annotation.tailrec
import scala.util.Success

import akka.NotUsed
import akka.stream._
import akka.stream.TLSProtocol._
import akka.stream.scaladsl._
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ AkkaSpec, WithLogCapturing }
import akka.util.ByteString

object TlsStageTruncationRegressionSpec {

  /**
   * Identity flow that, when its downstream cancels, does NOT propagate the
   * cancellation upstream; instead it keeps pulling and discarding. Used to
   * keep the client's `cipherOut` open after the peer goes away, so the client
   * is left in a genuine half-closed state (transport read side EOF, write side
   * still open) instead of being torn down by a cancellation cascade.
   */
  final class SwallowDownstreamCancel extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in: Inlet[ByteString] = Inlet("SwallowDownstreamCancel.in")
    val out: Outlet[ByteString] = Outlet("SwallowDownstreamCancel.out")
    override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

    override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = push(out, grab(in))
        override def onUpstreamFinish(): Unit = complete(out)
        override def onUpstreamFailure(ex: Throwable): Unit = fail(out, ex)
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
        override def onDownstreamFinish(cause: Throwable): Unit = {
          // Swallow: keep the upstream alive by draining it into the void.
          setHandler(in, new InHandler {
            override def onPush(): Unit = { grab(in); pull(in) }
            override def onUpstreamFinish(): Unit = completeStage()
            override def onUpstreamFailure(ex: Throwable): Unit = failStage(ex)
          })
          if (!hasBeenPulled(in)) pull(in)
        }
      })
    }
  }
}

/**
 * Regression test for the TLS bidi: a long-lived connection whose user-side
 * input (`plainIn`) stays open must still tear down when the transport read
 * side (`cipherIn`) reaches EOF without a TLS close_notify (an "unclean"/
 * truncated transport close, e.g. a half-closed TCP peer that dropped the
 * socket).
 *
 * The legacy `TLSActor` detects `TransportIn` depletion and runs
 * `closeInbound()` (emitting `SessionTruncated`) and completes. The new
 * `TlsStage` only consults `transportInAtEnd` from inside `doInbound`, which in
 * the `Bidirectional` phase is reached only when `outboundReady`, `inboundReady`
 * or `userInAtEnd` holds — none of which is true for an otherwise idle
 * connection with `plainIn` still open. The stage then never reacts to the EOF
 * and hangs.
 *
 * Run against both implementations: the actor-based variant passes, the
 * graph-stage variant fails (times out).
 */
abstract class TlsStageTruncationRegressionSpec
    extends AkkaSpec(TlsStageEdgeCasesSpec.configOverrides)
    with WithLogCapturing {
  import TlsStageEdgeCasesSpec._
  import TlsStageTruncationRegressionSpec._

  private val ctx = initContext()

  private def mkEngine(role: TLSRole): SSLEngine = {
    val engine = ctx.createSSLEngine()
    engine.setUseClientMode(role == Client)
    engine.setEnabledCipherSuites(TLS12Ciphers.toArray)
    engine.setEnabledProtocols(Array("TLSv1.2"))
    engine
  }

  /** Provided by the concrete subclasses to select the implementation under test. */
  protected def tls(
      engineFactory: () => SSLEngine,
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed]

  private def clientTls(closing: TLSClosing) = tls(() => mkEngine(Client), closing)
  private def serverTls(closing: TLSClosing) = tls(() => mkEngine(Server), closing)

  @tailrec
  private def drainUntilTerminated(sub: TestSubscriber.Probe[SslTlsInbound]): Unit = {
    sub.request(1)
    sub.expectNextOrComplete() match {
      case Left(_)  => () // OnComplete — stage tore down as expected
      case Right(_) => drainUntilTerminated(sub) // trailing SessionTruncated/SessionBytes
    }
  }

  "The TLS bidi" should {

    "tear down when the transport is truncated while plainIn stays open" in {
      val ks = KillSwitches.shared("trunc-ks")

      // Outbound (client -> server) keeps the client's cipherOut alive even
      // when the server tears down; inbound (server -> client) carries the
      // kill switch that we trip to simulate an unclean transport EOF.
      val terminator =
        BidiFlow.fromFlows(Flow[ByteString].via(new SwallowDownstreamCancel), ks.flow[ByteString])

      val echo = Flow[SslTlsInbound].collect { case SessionBytes(_, b) => SendBytes(b) }

      val tlsFlow =
        clientTls(IgnoreComplete).atop(terminator).atop(serverTls(IgnoreComplete).reversed).join(echo)

      // plainIn emits one element and then stays open forever (Source.never).
      val sub =
        Source
          .single[SslTlsOutbound](SendBytes(ByteString("hello")))
          .concat(Source.never[SslTlsOutbound])
          .via(tlsFlow)
          .runWith(TestSink[SslTlsInbound]())

      // Drive handshake + round-trip: wait until the echoed "hello" comes back.
      sub.request(10)
      var echoed = ByteString.empty
      while (echoed.utf8String != "hello") {
        sub.expectNext() match {
          case SessionBytes(_, b) => echoed ++= b
          case other              => fail(s"unexpected inbound element before truncation: $other")
        }
      }

      // Now truncate the transport read side without a close_notify, keeping
      // plainIn open. The stage must still tear plainOut down.
      ks.shutdown()

      drainUntilTerminated(sub)
    }
  }
}

/** Control: legacy actor-based implementation — expected to pass. */
class TlsStageTruncationActorSpec extends TlsStageTruncationRegressionSpec {
  protected def tls(
      engineFactory: () => SSLEngine,
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    TLS(engineFactory, closing)
}

/** New graph-stage implementation — currently fails (hangs) on truncation. */
class TlsStageTruncationGraphStageSpec extends TlsStageTruncationRegressionSpec {
  protected def tls(
      engineFactory: () => SSLEngine,
      closing: TLSClosing): BidiFlow[SslTlsOutbound, ByteString, ByteString, SslTlsInbound, NotUsed] =
    TLS.graphStageApply(engineFactory, _ => Success(()), closing)
}
