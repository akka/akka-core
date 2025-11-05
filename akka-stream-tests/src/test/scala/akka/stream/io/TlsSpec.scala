/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.util.concurrent.TimeoutException
import javax.net.ssl._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.NotUsed
import akka.pattern.{ after => later }
import akka.stream._
import akka.stream.TLSProtocol._
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream.testkit._
import akka.testkit.TestDuration
import akka.testkit.WithLogCapturing
import akka.util.{ ByteString, JavaVersion }

object TlsSpec {

  val rnd = new Random

  val TLS12Ciphers: Set[String] = Set("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA")
  val TLS13Ciphers: Set[String] = Set("TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384")

  def initWithTrust(trustPath: String, protocol: String): SSLContext = {
    val password = "changeme"

    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(getClass.getResourceAsStream("/keystore"), password.toCharArray)

    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(getClass.getResourceAsStream(trustPath), password.toCharArray)

    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password.toCharArray)

    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(trustStore)

    val context = SSLContext.getInstance(protocol)
    context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
    context
  }

  def initSslContext(protocol: String): SSLContext = initWithTrust("/truststore", protocol)

  /**
   * This is an operator that fires a TimeoutException failure 2 seconds after it was started,
   * independent of the traffic going through. The purpose is to include the last seen
   * element in the exception message to help in figuring out what went wrong.
   */
  class Timeout(duration: FiniteDuration) extends GraphStage[FlowShape[ByteString, ByteString]] {

    private val in = Inlet[ByteString]("in")
    private val out = Outlet[ByteString]("out")
    override val shape = FlowShape(in, out)

    override def createLogic(attr: Attributes) = new TimerGraphStageLogic(shape) {
      override def preStart(): Unit = scheduleOnce((), duration)

      var last: ByteString = _
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          last = grab(in)
          push(out, last)
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
      override def onTimer(x: Any): Unit = {
        failStage(new TimeoutException(s"timeout expired, last element was $last"))
      }
    }
  }

  val configOverrides =
    """
      akka.loglevel = DEBUG # issue 21660
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.debug.receive=off
    """
}

class TlsSpec extends StreamSpec(TlsSpec.configOverrides) with WithLogCapturing {
  import GraphDSL.Implicits._
  import TlsSpec._
  import system.dispatcher

  "SslTls" must {
    "work for TLSv1.2" must { workFor("TLSv1.2", TLS12Ciphers) }

    if (JavaVersion.majorVersion >= 11)
      "work for TLSv1.3" must { workFor("TLSv1.3", TLS13Ciphers) }

    def workFor(protocol: String, ciphers: Set[String]): Unit = {
      val sslContext = initSslContext(protocol)

      val debug = Flow[SslTlsInbound].map { x =>
        x match {
          case SessionTruncated   => system.log.debug(s" ----------- truncated ")
          case SessionBytes(_, b) => system.log.debug(s" ----------- (${b.size}) ${b.take(32).utf8String}")
        }
        x
      }

      def createSSLEngine(context: SSLContext, role: TLSRole): SSLEngine =
        createSSLEngine2(context, role, hostnameVerification = false, hostInfo = None)

      def createSSLEngine2(
          context: SSLContext,
          role: TLSRole,
          hostnameVerification: Boolean,
          hostInfo: Option[(String, Int)]): SSLEngine = {

        val engine = hostInfo match {
          case None =>
            if (hostnameVerification)
              throw new IllegalArgumentException("hostInfo must be defined for hostnameVerification to work.")
            context.createSSLEngine()
          case Some((hostname, port)) => context.createSSLEngine(hostname, port)
        }

        if (hostnameVerification && role == akka.stream.Client) {
          val sslParams = sslContext.getDefaultSSLParameters
          sslParams.setEndpointIdentificationAlgorithm("HTTPS")
          engine.setSSLParameters(sslParams)
        }

        engine.setUseClientMode(role == akka.stream.Client)
        engine.setEnabledCipherSuites(ciphers.toArray)
        engine.setEnabledProtocols(Array(protocol))

        engine
      }

      def clientTls(closing: TLSClosing) =
        TLS(() => createSSLEngine(sslContext, Client), closing)

      def badClientTls(closing: TLSClosing) =
        TLS(() => createSSLEngine(initWithTrust("/badtruststore", protocol), Client), closing)

      def serverTls(closing: TLSClosing) =
        TLS(() => createSSLEngine(sslContext, Server), closing)

      trait Named {
        def name: String =
          getClass.getName.reverse.dropWhile(c => "$0123456789".indexOf(c) != -1).takeWhile(_ != '$').reverse
      }

      trait CommunicationSetup extends Named {
        def decorateFlow(
            leftClosing: TLSClosing,
            rightClosing: TLSClosing,
            rhs: Flow[SslTlsInbound, SslTlsOutbound, Any]): Flow[SslTlsOutbound, SslTlsInbound, NotUsed]
        def cleanup(): Unit = ()
      }

      object ClientInitiates extends CommunicationSetup {
        def decorateFlow(
            leftClosing: TLSClosing,
            rightClosing: TLSClosing,
            rhs: Flow[SslTlsInbound, SslTlsOutbound, Any]) =
          clientTls(leftClosing).atop(serverTls(rightClosing).reversed).join(rhs)
      }

      object ServerInitiates extends CommunicationSetup {
        def decorateFlow(
            leftClosing: TLSClosing,
            rightClosing: TLSClosing,
            rhs: Flow[SslTlsInbound, SslTlsOutbound, Any]) =
          serverTls(leftClosing).atop(clientTls(rightClosing).reversed).join(rhs)
      }

      def server(flow: Flow[ByteString, ByteString, Any]) = {
        val server = Tcp(system).bind("localhost", 0).to(Sink.foreach(c => c.flow.join(flow).run())).run()
        Await.result(server, 2.seconds)
      }

      object ClientInitiatesViaTcp extends CommunicationSetup {
        var binding: Tcp.ServerBinding = null
        def decorateFlow(
            leftClosing: TLSClosing,
            rightClosing: TLSClosing,
            rhs: Flow[SslTlsInbound, SslTlsOutbound, Any]) = {
          binding = server(serverTls(rightClosing).reversed.join(rhs))
          clientTls(leftClosing).join(Tcp(system).outgoingConnection(binding.localAddress))
        }
        override def cleanup(): Unit = binding.unbind()
      }

      object ServerInitiatesViaTcp extends CommunicationSetup {
        var binding: Tcp.ServerBinding = null
        def decorateFlow(
            leftClosing: TLSClosing,
            rightClosing: TLSClosing,
            rhs: Flow[SslTlsInbound, SslTlsOutbound, Any]) = {
          binding = server(clientTls(rightClosing).reversed.join(rhs))
          serverTls(leftClosing).join(Tcp(system).outgoingConnection(binding.localAddress))
        }
        override def cleanup(): Unit = binding.unbind()
      }

      val communicationPatterns =
        Seq(ClientInitiates, ServerInitiates, ClientInitiatesViaTcp, ServerInitiatesViaTcp)

      trait PayloadScenario extends Named {
        def flow: Flow[SslTlsInbound, SslTlsOutbound, Any] =
          Flow[SslTlsInbound].map {
            var session: SSLSession = null
            def setSession(s: SSLSession) = {
              session = s
              system.log.debug(s"new session: $session (${session.getId.mkString(",")})")
            }

            {
              case SessionTruncated => SendBytes(ByteString("TRUNCATED"))
              case SessionBytes(s, b) if session == null =>
                setSession(s)
                SendBytes(b)
              case SessionBytes(s, b) if s != session =>
                setSession(s)
                SendBytes(ByteString("NEWSESSION") ++ b)
              case SessionBytes(_, b) => SendBytes(b)
            }
          }
        def leftClosing: TLSClosing = IgnoreComplete
        def rightClosing: TLSClosing = IgnoreComplete

        def inputs: immutable.Seq[SslTlsOutbound]
        def output: ByteString

        protected def send(str: String) = SendBytes(ByteString(str))
        protected def send(ch: Char) = SendBytes(ByteString(ch.toByte))
      }

      object SingleBytes extends PayloadScenario {
        val str = "0123456789"
        def inputs = str.map(ch => SendBytes(ByteString(ch.toByte)))
        def output = ByteString(str)
      }

      object MediumMessages extends PayloadScenario {
        val strs = "0123456789".map(d => d.toString * (rnd.nextInt(9000) + 1000))
        def inputs = strs.map(s => SendBytes(ByteString(s)))
        def output = ByteString(strs.foldRight("")(_ ++ _))
      }

      object LargeMessages extends PayloadScenario {
        // TLS max packet size is 16384 bytes
        val strs = "0123456789".map(d => d.toString * (rnd.nextInt(9000) + 17000))
        def inputs = strs.map(s => SendBytes(ByteString(s)))
        def output = ByteString(strs.foldRight("")(_ ++ _))
      }

      object EmptyBytesFirst extends PayloadScenario {
        def inputs = List(ByteString.empty, ByteString("hello")).map(SendBytes.apply)
        def output = ByteString("hello")
      }

      object EmptyBytesInTheMiddle extends PayloadScenario {
        def inputs = List(ByteString("hello"), ByteString.empty, ByteString(" world")).map(SendBytes.apply)
        def output = ByteString("hello world")
      }

      object EmptyBytesLast extends PayloadScenario {
        def inputs = List(ByteString("hello"), ByteString.empty).map(SendBytes.apply)
        def output = ByteString("hello")
      }

      object CompletedImmediately extends PayloadScenario {
        override def inputs: immutable.Seq[SslTlsOutbound] = Nil
        override def output = ByteString.empty

        override def leftClosing: TLSClosing = EagerClose
        override def rightClosing: TLSClosing = EagerClose
      }

      // this demonstrates that cancellation is ignored so that the five results make it back
      object CancellingRHS extends PayloadScenario {
        override def flow =
          Flow[SslTlsInbound]
            .mapConcat {
              case SessionTruncated       => SessionTruncated :: Nil
              case SessionBytes(s, bytes) => bytes.map(b => SessionBytes(s, ByteString(b)))
            }
            .take(5)
            .mapAsync(5)(x => later(500.millis, system.scheduler)(Future.successful(x)))
            .via(super.flow)
        override def rightClosing = IgnoreCancel

        val str = "abcdef" * 100
        def inputs = str.map(send)
        def output = ByteString(str.take(5))
      }

      object CancellingRHSIgnoresBoth extends PayloadScenario {
        override def flow =
          Flow[SslTlsInbound]
            .mapConcat {
              case SessionTruncated       => SessionTruncated :: Nil
              case SessionBytes(s, bytes) => bytes.map(b => SessionBytes(s, ByteString(b)))
            }
            .take(5)
            .mapAsync(5)(x => later(500.millis, system.scheduler)(Future.successful(x)))
            .via(super.flow)
        override def rightClosing = IgnoreBoth
        val str = "abcdef" * 100
        def inputs = str.map(send)
        def output = ByteString(str.take(5))
      }

      object LHSIgnoresBoth extends PayloadScenario {
        override def leftClosing = IgnoreBoth
        val str = "0123456789"
        def inputs = str.map(ch => SendBytes(ByteString(ch.toByte)))
        def output = ByteString(str)
      }

      object BothSidesIgnoreBoth extends PayloadScenario {
        override def leftClosing = IgnoreBoth
        override def rightClosing = IgnoreBoth
        val str = "0123456789"
        def inputs = str.map(ch => SendBytes(ByteString(ch.toByte)))
        def output = ByteString(str)
      }

      object SessionRenegotiationBySender extends PayloadScenario {
        def inputs = List(send("hello"), NegotiateNewSession, send("world"))
        def output = ByteString("helloNEWSESSIONworld")
      }

      // difference is that the RHS engine will now receive the handshake while trying to send
      object SessionRenegotiationByReceiver extends PayloadScenario {
        val str = "abcdef" * 100
        def inputs = str.map(send) ++ Seq(NegotiateNewSession) ++ "hello world".map(send)
        def output = ByteString(str + "NEWSESSIONhello world")
      }

      val logCipherSuite = Flow[SslTlsInbound].map {
        var session: SSLSession = null
        def setSession(s: SSLSession) = {
          session = s
          system.log.debug(s"new session: $session (${session.getId.mkString(",")})")
        }

        {
          case SessionTruncated => SendBytes(ByteString("TRUNCATED"))
          case SessionBytes(s, b) if s != session =>
            setSession(s)
            SendBytes(ByteString(s.getCipherSuite) ++ b)
          case SessionBytes(_, b) => SendBytes(b)
        }
      }

      object SessionRenegotiationFirstOne extends PayloadScenario {
        override def flow = logCipherSuite
        def inputs = NegotiateNewSession.withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA") :: send("hello") :: Nil
        def output = ByteString("TLS_RSA_WITH_AES_128_CBC_SHAhello")
      }

      object SessionRenegotiationFirstTwo extends PayloadScenario {
        override def flow = logCipherSuite
        def inputs = NegotiateNewSession.withCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA") :: send("hello") :: Nil
        def output = ByteString("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHAhello")
      }

      val scenarios =
        Seq(
          SingleBytes,
          MediumMessages,
          LargeMessages,
          EmptyBytesFirst,
          EmptyBytesInTheMiddle,
          EmptyBytesLast,
          CompletedImmediately,
          CancellingRHS,
          CancellingRHSIgnoresBoth,
          LHSIgnoresBoth,
          BothSidesIgnoreBoth) ++
        (if (protocol == "TLSv1.2")
           Seq(
             SessionRenegotiationBySender,
             SessionRenegotiationByReceiver,
             SessionRenegotiationFirstOne,
             SessionRenegotiationFirstTwo)
         else // TLSv1.3 doesn't support renegotiation
           Nil)

      for {
        commPattern <- communicationPatterns
        scenario <- scenarios
      } {
        s"work in mode ${commPattern.name} while sending ${scenario.name}" in {
          val onRHS = debug.via(scenario.flow)
          val output =
            Source(scenario.inputs)
              .via(commPattern.decorateFlow(scenario.leftClosing, scenario.rightClosing, onRHS))
              .via(new SimpleLinearGraphStage[SslTlsInbound] {
                override def createLogic(inheritedAttributes: Attributes) =
                  new GraphStageLogic(shape) with InHandler with OutHandler {
                    setHandlers(in, out, this)

                    override def onPush() = push(out, grab(in))
                    override def onPull() = pull(in)

                    override def onDownstreamFinish(cause: Throwable) = {
                      system.log.debug(s"me cancelled, cause {}", cause)
                      completeStage()
                    }
                  }
              })
              .via(debug)
              .collect { case SessionBytes(_, b) => b }
              .scan(ByteString.empty)(_ ++ _)
              .filter(_.nonEmpty)
              .via(new Timeout(10.seconds))
              .dropWhile(_.size < scenario.output.size)
              .runWith(Sink.headOption)

          Await.result(output, 12.seconds).getOrElse(ByteString.empty).utf8String should be(scenario.output.utf8String)

          commPattern.cleanup()
        }
      }

      "emit an error if the TLS handshake fails certificate checks" in {
        val getError = Flow[SslTlsInbound]
          .map[Either[SslTlsInbound, SSLException]](i => Left(i))
          .recover { case e: SSLException => Right(e) }
          .collect { case Right(e) => e }
          .toMat(Sink.head)(Keep.right)

        val simple = Flow.fromSinkAndSourceMat(getError, Source.maybe[SslTlsOutbound])(Keep.left)

        // The creation of actual TCP connections is necessary. It is the easiest way to decouple the client and server
        // under error conditions, and has the bonus of matching most actual SSL deployments.
        val (server, serverErr) = Tcp(system)
          .bind("localhost", 0)
          .mapAsync(1)(c =>
            c.flow.joinMat(serverTls(IgnoreBoth).reversed.joinMat(simple)(Keep.right))(Keep.right).run())
          .toMat(Sink.head)(Keep.both)
          .run()

        val clientErr = simple
          .join(badClientTls(IgnoreBoth))
          .join(Tcp(system).outgoingConnection(Await.result(server, 1.second).localAddress))
          .run()

        Await.result(serverErr, 1.second).getMessage should include("certificate_unknown")

        val clientErrText = rootCauseOf(Await.result(clientErr, 1.second)).getMessage
        clientErrText should include("unable to find valid certification path to requested target")
      }

      "reliably cancel subscriptions when TransportIn fails early" in {
        val ex = new Exception("hello")
        val (sub, out1, out2) =
          RunnableGraph
            .fromGraph(
              GraphDSL.createGraph(
                Source.asSubscriber[SslTlsOutbound],
                Sink.head[ByteString],
                Sink.head[SslTlsInbound])((_, _, _)) { implicit b => (s, o1, o2) =>
                val tls = b.add(clientTls(EagerClose))
                s ~> tls.in1
                tls.out1 ~> o1
                o2 <~ tls.out2
                tls.in2 <~ Source.failed(ex)
                ClosedShape
              })
            .run()
        the[Exception] thrownBy Await.result(out1, 1.second) should be(ex)
        the[Exception] thrownBy Await.result(out2, 1.second) should be(ex)
        Thread.sleep(500)
        val pub = TestPublisher.probe()
        pub.subscribe(sub)
        pub.expectSubscription().expectCancellation()
      }

      "reliably cancel subscriptions when UserIn fails early" in {
        val ex = new Exception("hello")
        val (sub, out1, out2) =
          RunnableGraph
            .fromGraph(
              GraphDSL.createGraph(Source.asSubscriber[ByteString], Sink.head[ByteString], Sink.head[SslTlsInbound])(
                (_, _, _)) { implicit b => (s, o1, o2) =>
                val tls = b.add(clientTls(EagerClose))
                Source.failed[SslTlsOutbound](ex) ~> tls.in1
                tls.out1 ~> o1
                o2 <~ tls.out2
                tls.in2 <~ s
                ClosedShape
              })
            .run()
        the[Exception] thrownBy Await.result(out1, 1.second) should be(ex)
        the[Exception] thrownBy Await.result(out2, 1.second) should be(ex)
        Thread.sleep(500)
        val pub = TestPublisher.probe()
        pub.subscribe(sub)
        pub.expectSubscription().expectCancellation()
      }

      "complete if TLS connection is truncated" in {

        val ks = KillSwitches.shared("ks")

        val scenario = SingleBytes

        val outFlow = {
          val terminator = BidiFlow.fromFlows(Flow[ByteString], ks.flow[ByteString])
          clientTls(scenario.leftClosing)
            .atop(terminator)
            .atop(serverTls(scenario.rightClosing).reversed)
            .join(debug.via(scenario.flow))
            .via(debug)
        }

        val inFlow = Flow[SslTlsInbound]
          .collect { case SessionBytes(_, b) => b }
          .scan(ByteString.empty)(_ ++ _)
          .via(new Timeout(6.seconds.dilated))
          .dropWhile(_.size < scenario.output.size)

        val f =
          Source(scenario.inputs)
            .via(outFlow)
            .via(inFlow)
            .map(result => {
              ks.shutdown()
              result
            })
            .runWith(Sink.last)
        Await.result(f, 8.second.dilated).utf8String should be(scenario.output.utf8String)
      }

      "verify hostname" in {
        def run(hostName: String): Future[akka.Done] = {
          val rhs = Flow[SslTlsInbound].map {
            case SessionTruncated   => SendBytes(ByteString.empty)
            case SessionBytes(_, b) => SendBytes(b)
          }
          val clientTls = TLS(
            () => createSSLEngine2(sslContext, Client, hostnameVerification = true, hostInfo = Some((hostName, 80))),
            EagerClose)

          val flow = clientTls.atop(serverTls(EagerClose).reversed).join(rhs)

          Source.single(SendBytes(ByteString.empty)).via(flow).runWith(Sink.ignore)
        }

        Await.result(run("akka-remote"), 3.seconds) // CN=akka-remote
        val cause = intercept[Exception] {
          Await.result(run("unknown.example.org"), 3.seconds)
        }

        cause.getClass should ===(classOf[SSLHandshakeException]) //General SSLEngine problem

        val rootCause = rootCauseOf(cause.getCause)
        rootCause.getClass should ===(classOf[CertificateException])
        rootCause.getMessage should ===("No name matching unknown.example.org found")
      }
    }
  }

  def rootCauseOf(e: Throwable): Throwable = {
    if (JavaVersion.majorVersion >= 11) e
    // Wrapped in extra 'General SSLEngine problem' (sometimes multiple)
    // on 1.8.0-265 and before, but not 1.8.0-272 and later...
    else if (e.isInstanceOf[SSLHandshakeException]) rootCauseOf(e.getCause)
    else e
  }

  "A SslTlsPlacebo" must {

    "pass through data" in {
      val f = Source(1 to 3)
        .map(b => SendBytes(ByteString(b.toByte)))
        .via(TLSPlacebo().join(Flow.apply))
        .grouped(10)
        .runWith(Sink.head)
      val result = Await.result(f, 3.seconds)
      result.map(_.bytes) should be((1 to 3).map(b => ByteString(b.toByte)))
      result.map(_.session).foreach(s => s.getCipherSuite should be("SSL_NULL_WITH_NULL_NULL"))
    }

  }

}
