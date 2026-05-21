/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.security.KeyStore
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.net.ssl._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.TLSProtocol._
import akka.stream.scaladsl._
import akka.util.ByteString

object TlsBenchmark {

  val config = ConfigFactory.parseString("""
      akka {
        loglevel = "WARNING"
        actor.default-dispatcher {
          throughput = 1024
        }
        actor.default-mailbox {
          mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
        }
      }""".stripMargin).withFallback(ConfigFactory.load())

  private val password = "changeme".toCharArray

  def initSslContext(): SSLContext = {
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(classOf[TlsThroughputBenchmark].getResourceAsStream("/tls-bench/keystore"), password)
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(classOf[TlsThroughputBenchmark].getResourceAsStream("/tls-bench/truststore"), password)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(keyStore, password)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(trustStore)
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(kmf.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    ctx
  }

  def mkEngine(sslContext: SSLContext, role: TLSRole): SSLEngine = {
    val e = sslContext.createSSLEngine()
    e.setUseClientMode(role == Client)
    e
  }

  /** Client TLS atop reversed server TLS with an echo flow that bounces
   *  decrypted SessionBytes back as SendBytes. */
  def tlsEcho(sslContext: SSLContext, closing: TLSClosing): Flow[SslTlsOutbound, SslTlsInbound, akka.NotUsed] = {
    val echo = Flow[SslTlsInbound].collect { case SessionBytes(_, bytes) => SendBytes(bytes) }
    TLS(() => mkEngine(sslContext, Client), closing)
      .atop(TLS(() => mkEngine(sslContext, Server), closing).reversed)
      .join(echo)
  }
}

// ==================== 1. Throughput ====================

/**
 * Measures sustained byte throughput through a TLS loopback (client bidi
 * atop server bidi with an echo flow). Parameterised by payload size to
 * exercise different wrap/unwrap batching characteristics.
 *
 * The `simulateNetwork` parameter inserts an `.async` boundary between the
 * two TLS stages. With it, the topology more closely matches real Akka
 * usage (one TLS instance per JVM with a network in between). Without it,
 * both TLS stages run in the same fused interpreter, which prevents the
 * pipelining the old actor-based TlsModule got from being its own island.
 *
 * Pass `-Dakka.stream.materializer.io.tls.use-graph-stage-implementation=true`
 * to benchmark the new GraphStage-based TLS implementation instead of the
 * default actor-based one.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class TlsThroughputBenchmark {
  import TlsBenchmark._

  implicit var system: ActorSystem = _
  private var sslContext: SSLContext = _

  @Param(Array("64", "1024", "16384", "65536"))
  var payloadSize = 0

  private val messageCount = 1000
  private var messages: immutable.Seq[SslTlsOutbound] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem("tls-throughput-bench", config)
    SystemMaterializer(system).materializer
    sslContext = initSslContext()
    messages = Vector.fill(messageCount)(SendBytes(ByteString(new Array[Byte](payloadSize))))
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(1000)
  def throughput(): Unit = {
    val expectedBytes = payloadSize.toLong * messageCount
    val latch = new CountDownLatch(1)
    Source(messages)
      .via(tlsEcho(sslContext, IgnoreComplete))
      .collect { case SessionBytes(_, b) => b }
      .scan(ByteString.empty)(_ ++ _)
      .filter(_.size >= expectedBytes)
      .take(1) // cancel stream so prior iterations don't leak
      .to(Sink.foreach(_ => latch.countDown()))
      .run()

    if (!latch.await(60, TimeUnit.SECONDS))
      throw new RuntimeException("TLS throughput bench timed out")
  }
}

// ==================== 2. Handshake ====================

/**
 * Measures TLS handshake cost by materialising a fresh TLS loopback per
 * invocation and sending a single byte through (forcing the handshake to
 * complete before any application data flows).
 *
 * Pass `-Dakka.stream.materializer.io.tls.use-graph-stage-implementation=true`
 * to benchmark the new GraphStage-based TLS implementation instead of the
 * default actor-based one.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class TlsHandshakeBenchmark {
  import TlsBenchmark._

  implicit var system: ActorSystem = _
  private var sslContext: SSLContext = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem("tls-handshake-bench", config)
    SystemMaterializer(system).materializer
    sslContext = initSslContext()
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  def handshake(): Unit = {
    val latch = new CountDownLatch(1)
    Source
      .single(SendBytes(ByteString("x")))
      .via(tlsEcho(sslContext, EagerClose))
      .to(Sink.onComplete(_ => latch.countDown()))
      .run()

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("TLS handshake bench timed out")
  }
}

// ==================== 3. Framing (many small messages) ====================

/**
 * Pushes a large number of small messages through the TLS loopback to
 * stress the pump loop, chopping block, and demand management rather than
 * the SSLEngine crypto itself.
 *
 * Pass `-Dakka.stream.materializer.io.tls.use-graph-stage-implementation=true`
 * to benchmark the new GraphStage-based TLS implementation instead of the
 * default actor-based one.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class TlsFramingBenchmark {
  import TlsBenchmark._

  implicit var system: ActorSystem = _
  private var sslContext: SSLContext = _

  @Param(Array("10", "100"))
  var messageSize = 0

  private val messageCount = 10000
  private var messages: immutable.Seq[SslTlsOutbound] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem("tls-framing-bench", config)
    SystemMaterializer(system).materializer
    sslContext = initSslContext()
    messages = Vector.fill(messageCount)(SendBytes(ByteString(new Array[Byte](messageSize))))
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def framing(): Unit = {
    val expectedBytes = messageSize.toLong * messageCount
    val latch = new CountDownLatch(1)
    Source(messages)
      .via(tlsEcho(sslContext, IgnoreComplete))
      .collect { case SessionBytes(_, b) => b }
      .scan(ByteString.empty)(_ ++ _)
      .filter(_.size >= expectedBytes)
      .take(1) // cancel stream so prior iterations don't leak
      .to(Sink.foreach(_ => latch.countDown()))
      .run()

    if (!latch.await(60, TimeUnit.SECONDS))
      throw new RuntimeException("TLS framing bench timed out")
  }
}

// ==================== 4. Ping-pong (round-trip latency) ====================

/**
 * Serialised round-trip latency: sends one message, waits for the echo,
 * then sends the next. Each round trip exercises the full TLS wrap-unwrap
 * cycle under no pipelining, making it the most sensitive benchmark to
 * per-hop overhead (the async boundary this PR removes).
 *
 * Uses `Source.actorRef` with a feedback loop: on each received pong the
 * sink callback sends the next ping to the source actor, so exactly one
 * message is in flight at any time.
 *
 * Pass `-Dakka.stream.materializer.io.tls.use-graph-stage-implementation=true`
 * to benchmark the new GraphStage-based TLS implementation instead of the
 * default actor-based one.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class TlsPingPongBenchmark {
  import TlsBenchmark._

  implicit var system: ActorSystem = _
  private var sslContext: SSLContext = _

  private val roundTrips = 100

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem("tls-pingpong-bench", config)
    SystemMaterializer(system).materializer
    sslContext = initSslContext()
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(100)
  def pingPong(): Unit = {
    val ping = SendBytes(ByteString("ping"))
    val latch = new CountDownLatch(1)
    val remaining = new java.util.concurrent.atomic.AtomicInteger(roundTrips)
    val refHolder = new java.util.concurrent.atomic.AtomicReference[akka.actor.ActorRef]()

    val ((ref, killSwitch), _) = Source
      .actorRef[SslTlsOutbound](
        completionMatcher = PartialFunction.empty,
        failureMatcher = PartialFunction.empty,
        bufferSize = 1,
        overflowStrategy = OverflowStrategy.dropBuffer)
      .via(tlsEcho(sslContext, IgnoreComplete))
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.foreach {
        case SessionBytes(_, _) =>
          if (remaining.decrementAndGet() > 0) refHolder.get().tell(ping, akka.actor.ActorRef.noSender)
          else latch.countDown()
        case _ => ()
      })(Keep.both)
      .run()

    refHolder.set(ref)
    ref ! ping // kick off the first round trip

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("TLS ping-pong bench timed out")

    killSwitch.abort(new RuntimeException("done")) // tear down the stream to avoid leaking across iterations
  }
}
