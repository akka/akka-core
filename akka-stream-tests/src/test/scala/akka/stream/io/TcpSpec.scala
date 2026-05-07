/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.net._
import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import akka.Done
import akka.NotUsed
import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Identify
import akka.actor.Kill
import akka.io.Dns
import akka.io.DnsProvider
import akka.io.SimpleDnsCache
import akka.io.Tcp._
import akka.io.dns.DnsProtocol
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Tcp.IncomingConnection
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.stream.testkit._
import akka.testkit.EventFilter
import akka.testkit.SocketUtil.{ temporaryServerAddress, temporaryServerHostnameAndPort }
import akka.testkit.TestKit
import akka.testkit.TestLatch
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing
import akka.util.ByteString

@nowarn("msg=never used")
class NonResolvingDnsActor(cache: SimpleDnsCache, config: Config) extends Actor {
  def receive = {
    case msg =>
      throw new RuntimeException(s"Unexpected resolve message $msg")
  }
}

@nowarn("msg=never used")
class NonResolvingDnsManager(ext: akka.io.DnsExt) extends Actor {

  def receive = {
    case msg =>
      throw new RuntimeException(s"Unexpected resolve message $msg")
  }
}

@nowarn("msg=deprecated")
class FailingDnsResolver extends DnsProvider {
  override val cache: Dns = new Dns {
    override def cached(name: String): Option[Dns.Resolved] = None
    override def resolve(name: String)(system: ActorSystem, sender: ActorRef): Option[Dns.Resolved] = {
      // tricky impl detail this is actually where the resolve response is triggered
      // we fake that it fails directly from here
      sender ! Dns.Resolved(name, immutable.Seq.empty, immutable.Seq.empty)
      None
    }
    override def cached(request: DnsProtocol.Resolve): Option[DnsProtocol.Resolved] = None
    override def resolve(
        request: DnsProtocol.Resolve,
        system: ActorSystem,
        sender: ActorRef): Option[DnsProtocol.Resolved] = {
      sender ! DnsProtocol.Resolved(request.name, immutable.Seq.empty, immutable.Seq.empty)
      None
    }
  }
  override def actorClass = classOf[NonResolvingDnsActor]
  override def managerClass = classOf[NonResolvingDnsManager]
}

class TcpSpec extends StreamSpec("""
    akka.loglevel = debug
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.io.tcp.trace-logging = true
    akka.stream.materializer.subscription-timeout.timeout = 2s
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) with TcpHelper with WithLogCapturing {

  "Outgoing TCP stream" must {

    "work in the happy case" in {
      val testData = ByteString(1, 2, 3, 4, 5)

      val server = new Server()

      val tcpReadProbe = new TcpReadProbe()
      val tcpWriteProbe = new TcpWriteProbe()
      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      validateServerClientCommunication(testData, serverConnection, tcpReadProbe, tcpWriteProbe)

      tcpWriteProbe.close()
      tcpReadProbe.close()

      server.close()
    }

    "be able to write a sequence of ByteStrings" in {
      val server = new Server()
      val testInput = (0 to 255).map(ByteString(_))
      val expectedOutput = ByteString(Array.tabulate(256)(_.asInstanceOf[Byte]))

      Source(testInput).via(Tcp().outgoingConnection(server.address)).to(Sink.ignore).run()

      val serverConnection = server.waitAccept()
      serverConnection.read(256)
      serverConnection.waitRead() should be(expectedOutput)
    }

    "be able to read a sequence of ByteStrings" in {
      val server = new Server()
      val testInput = (0 to 255).map(ByteString(_))
      val expectedOutput = ByteString(Array.tabulate(256)(_.asInstanceOf[Byte]))

      val idle = new TcpWriteProbe() // Just register an idle upstream
      val resultFuture =
        Source
          .fromPublisher(idle.publisherProbe)
          .via(Tcp().outgoingConnection(server.address))
          .runFold(ByteString.empty)((acc, in) => acc ++ in)
      val serverConnection = server.waitAccept()

      for (in <- testInput) {
        serverConnection.write(in)
      }

      serverConnection.confirmedClose()
      Await.result(resultFuture, 3.seconds) should be(expectedOutput)

    }

    "fail the materialized future when the connection fails" in {
      val tcpWriteProbe = new TcpWriteProbe()
      val future = Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .viaMat(
          Tcp().outgoingConnection(InetSocketAddress.createUnresolved("example.com", 666), connectTimeout = 1.second))(
          Keep.right)
        .toMat(Sink.ignore)(Keep.left)
        .run()

      future.failed.futureValue shouldBe a[StreamTcpException]
    }

    "work when client closes write, then remote closes write" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()
      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      // Close client side write
      tcpWriteProbe.close()
      serverConnection.expectClosed(PeerClosed)

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Close server side write
      serverConnection.confirmedClose()
      tcpReadProbe.subscriberProbe.expectComplete()

      serverConnection.expectClosed(ConfirmedClosed)
      serverConnection.expectTerminated()
    }

    "work when remote closes write, then client closes write" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()
      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Close server side write
      serverConnection.confirmedClose()
      tcpReadProbe.subscriberProbe.expectComplete()

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      // Close client side write
      tcpWriteProbe.close()
      serverConnection.expectClosed(ConfirmedClosed)
      serverConnection.expectTerminated()
    }

    "work when client closes read, then client closes write" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()
      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Close client side read
      tcpReadProbe.tcpReadSubscription.cancel()

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      // Close client side write
      tcpWriteProbe.close()

      // Need a write on the server side to detect the close event
      awaitAssert({
        serverConnection.write(testData)
        serverConnection.expectClosed(_.isErrorClosed, 500.millis)
      }, max = 5.seconds)
      serverConnection.expectTerminated()
    }

    "work when client closes write, then client closes read" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()
      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      // Close client side write
      tcpWriteProbe.close()
      serverConnection.expectClosed(PeerClosed)

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Close client side read
      tcpReadProbe.tcpReadSubscription.cancel()

      // Need a write on the server side to detect the close event
      awaitAssert({
        serverConnection.write(testData)
        serverConnection.expectClosed(_.isErrorClosed, 500.millis)
      }, max = 5.seconds)
      serverConnection.expectTerminated()
    }

    "work when client closes read, then server closes write, then client closes write" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()
      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Close client side read
      tcpReadProbe.tcpReadSubscription.cancel()

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      serverConnection.confirmedClose()

      // Close client side write
      tcpWriteProbe.close()
      serverConnection.expectClosed(ConfirmedClosed)
      serverConnection.expectTerminated()
    }

    "shut everything down if client signals error" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()

      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      // Cause error
      tcpWriteProbe.tcpWriteSubscription.sendError(new IllegalStateException("test"))

      tcpReadProbe.subscriberProbe.expectError()
      serverConnection.expectClosed(_.isErrorClosed)
      serverConnection.expectTerminated()
    }

    "shut everything down if client signals error after remote has closed write" in {
      val testData = ByteString(1, 2, 3, 4, 5)
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()

      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      // Server can still write
      serverConnection.write(testData)
      tcpReadProbe.read(5) should be(testData)

      // Close remote side write
      serverConnection.confirmedClose()
      tcpReadProbe.subscriberProbe.expectComplete()

      // Client can still write
      tcpWriteProbe.write(testData)
      serverConnection.read(5)
      serverConnection.waitRead() should be(testData)

      tcpWriteProbe.tcpWriteSubscription.sendError(new IllegalStateException("test"))
      serverConnection.expectClosed(_.isErrorClosed)
      serverConnection.expectTerminated()
    }

    "shut down both streams when connection is aborted remotely" in {
      // Client gets a PeerClosed event and does not know that the write side is also closed
      val server = new Server()

      val tcpWriteProbe = new TcpWriteProbe()
      val tcpReadProbe = new TcpReadProbe()

      Source
        .fromPublisher(tcpWriteProbe.publisherProbe)
        .via(Tcp().outgoingConnection(server.address))
        .to(Sink.fromSubscriber(tcpReadProbe.subscriberProbe))
        .run()
      val serverConnection = server.waitAccept()

      serverConnection.abort()
      tcpReadProbe.subscriberProbe.expectSubscriptionAndError()
      tcpWriteProbe.tcpWriteSubscription.expectCancellation()

      serverConnection.expectTerminated()
    }

    "materialize correctly when used in multiple flows" in {
      val testData = ByteString(1, 2, 3, 4, 5)

      val server = new Server()

      val tcpReadProbe1 = new TcpReadProbe()
      val tcpWriteProbe1 = new TcpWriteProbe()
      val tcpReadProbe2 = new TcpReadProbe()
      val tcpWriteProbe2 = new TcpWriteProbe()
      val outgoingConnection = Tcp().outgoingConnection(server.address)

      val conn1F =
        Source
          .fromPublisher(tcpWriteProbe1.publisherProbe)
          .viaMat(outgoingConnection)(Keep.right)
          .to(Sink.fromSubscriber(tcpReadProbe1.subscriberProbe))
          .run()
      val serverConnection1 = server.waitAccept()
      val conn2F =
        Source
          .fromPublisher(tcpWriteProbe2.publisherProbe)
          .viaMat(outgoingConnection)(Keep.right)
          .to(Sink.fromSubscriber(tcpReadProbe2.subscriberProbe))
          .run()
      val serverConnection2 = server.waitAccept()

      validateServerClientCommunication(testData, serverConnection1, tcpReadProbe1, tcpWriteProbe1)
      validateServerClientCommunication(testData, serverConnection2, tcpReadProbe2, tcpWriteProbe2)

      val conn1 = conn1F.futureValue
      val conn2 = conn2F.futureValue

      // Since we have already communicated over the connections we can have short timeouts for the futures
      conn1.remoteAddress.getPort should be(server.address.getPort)
      conn2.remoteAddress.getPort should be(server.address.getPort)
      conn1.localAddress.getPort should not be conn2.localAddress.getPort

      tcpWriteProbe1.close()
      tcpReadProbe1.close()

      server.close()
    }

    "properly half-close by default" in {
      val writeButDontRead: Flow[ByteString, ByteString, NotUsed] =
        Flow.fromSinkAndSource(Sink.cancelled, Source.single(ByteString("Early response")))

      val binding =
        Tcp()
          .bind("127.0.0.1", 0, halfClose = true)
          .toMat(Sink.foreach { conn =>
            conn.flow.join(writeButDontRead).run()
          })(Keep.left)
          .run()
          .futureValue

      val result = Source.empty
        .via(Tcp().outgoingConnection(binding.localAddress))
        .toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right)
        .run()

      result.futureValue should ===(ByteString("Early response"))

      binding.unbind()
    }

    "properly full-close if requested" in {
      val writeButIgnoreRead: Flow[ByteString, ByteString, NotUsed] =
        Flow.fromSinkAndSourceMat(Sink.ignore, Source.single(ByteString("Early response")))(Keep.right)

      val binding =
        Tcp()
          .bind("127.0.0.1", 0, halfClose = false)
          .toMat(Sink.foreach { conn =>
            conn.flow.join(writeButIgnoreRead).run()
          })(Keep.left)
          .run()
          .futureValue

      val (promise, result) = Source
        .maybe[ByteString]
        .via(Tcp().outgoingConnection(binding.localAddress))
        .toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.both)
        .run()

      result.futureValue should ===(ByteString("Early response"))

      promise.success(None) // close client upstream, no more data
      binding.unbind()
    }

    "Echo should work even if server is in full close mode" in {
      val serverAddress = temporaryServerAddress()

      val binding =
        Tcp()
          .bind(serverAddress.getHostString, serverAddress.getPort, halfClose = false)
          .toMat(Sink.foreach { conn =>
            conn.flow.join(Flow[ByteString]).run()
          })(Keep.left)
          .run()
          .futureValue

      val result = Source(immutable.Iterable.fill(1000)(ByteString(0)))
        .via(Tcp().outgoingConnection(serverAddress, halfClose = true))
        .runFold(0)(_ + _.size)

      result.futureValue should ===(1000)

      binding.unbind()
    }

    "handle when connection actor terminates unexpectedly" in {
      val system2 = ActorSystem(
        "TcpSpec-unexpected-system2",
        ConfigFactory.parseString("""
          akka.loglevel = DEBUG # issue #21660
        """).withFallback(system.settings.config))

      try {
        implicit val materializer = SystemMaterializer(system2).materializer

        val serverAddress = temporaryServerAddress()
        val binding =
          Tcp(system2).bindAndHandle(Flow[ByteString], serverAddress.getHostString, serverAddress.getPort).futureValue

        val probe = TestProbe()
        val testMsg = ByteString(0)
        val result =
          Source
            .single(testMsg)
            .concat(Source.maybe[ByteString])
            .via(Tcp(system2).outgoingConnection(serverAddress))
            .runForeach { msg =>
              probe.ref ! msg
            }

        // Ensure first that the actor is there
        probe.expectMsg(testMsg)

        // Getting rid of existing connection actors by using a blunt instrument
        val path = akka.io.Tcp(system2).getManager.path / "selectors" / s"$$a" / "*"

        // Some more verbose info when #21839 happens again
        system2.actorSelection(path).tell(Identify(()), probe.ref)
        try {
          probe.expectMsgType[ActorIdentity].ref.get
        } catch {
          case _: AssertionError | _: NoSuchElementException =>
            val tree = system2.asInstanceOf[ExtendedActorSystem].printTree
            fail(s"No TCP selector actor running at [$path], actor tree: $tree")
        }
        system2.actorSelection(path) ! Kill

        result.failed.futureValue shouldBe a[StreamTcpException]

        binding.unbind()
      } finally {
        TestKit.shutdownActorSystem(system2)
      }
    }

    "provide full exceptions when connection attempt fails because name cannot be resolved" in {
      val systemWithBrokenDns = ActorSystem(
        "TcpSpec-resolution-failure",
        ConfigFactory.parseString("""
          akka.io.dns.inet-address.provider-object = akka.stream.io.FailingDnsResolver
          """).withFallback(system.settings.config))
      try {
        val unknownHostName = "abcdefghijklmnopkuh"

        val test =
          Source.maybe
            .viaMat(Tcp(systemWithBrokenDns).outgoingConnection(unknownHostName, 12345))(Keep.right)
            .to(Sink.ignore)
            .run()(SystemMaterializer(systemWithBrokenDns).materializer)
            .failed
            .futureValue

        test.getCause shouldBe a[UnknownHostException]

      } finally {
        TestKit.shutdownActorSystem(systemWithBrokenDns)
      }
    }
  }

  "TCP listen stream" must {

    // Reusing handler
    val echoHandler = Sink.foreach[Tcp.IncomingConnection] { _.flow.join(Flow[ByteString]).run() }

    "be able to implement echo" in {
      val serverAddress = temporaryServerAddress()
      val (bindingFuture, echoServerFinish) =
        Tcp().bind(serverAddress.getHostString, serverAddress.getPort).toMat(echoHandler)(Keep.both).run()

      // make sure that the server has bound to the socket
      val binding = bindingFuture.futureValue

      val testInput = (0 to 255).map(ByteString(_))
      val expectedOutput = ByteString(Array.tabulate(256)(_.asInstanceOf[Byte]))
      val resultFuture =
        Source(testInput).via(Tcp().outgoingConnection(serverAddress)).runFold(ByteString.empty)((acc, in) => acc ++ in)

      binding.whenUnbound.value should be(None)
      resultFuture.futureValue should be(expectedOutput)
      binding.unbind().futureValue
      echoServerFinish.futureValue
      binding.whenUnbound.futureValue should be(Done)
    }

    "work with a chain of echoes" in {
      val serverAddress = temporaryServerAddress()
      val (bindingFuture, echoServerFinish) =
        Tcp().bind(serverAddress.getHostString, serverAddress.getPort).toMat(echoHandler)(Keep.both).run()

      // make sure that the server has bound to the socket
      val binding = bindingFuture.futureValue
      binding.whenUnbound.value should be(None)

      val echoConnection = Tcp().outgoingConnection(serverAddress)

      val testInput = (0 to 255).map(ByteString(_))
      val expectedOutput = ByteString(Array.tabulate(256)(_.asInstanceOf[Byte]))

      val resultFuture =
        Source(testInput)
          .via(echoConnection) // The echoConnection is reusable
          .via(echoConnection)
          .via(echoConnection)
          .via(echoConnection)
          .runFold(ByteString.empty)((acc, in) => acc ++ in)

      resultFuture.futureValue should be(expectedOutput)
      binding.unbind().futureValue
      echoServerFinish.futureValue
      binding.whenUnbound.futureValue should be(Done)
    }

    "bind and unbind correctly" in EventFilter[BindException](occurrences = 2).intercept {
      val address = temporaryServerAddress()
      val probe1 = TestSubscriber.manualProbe[Tcp.IncomingConnection]()
      val bind = Tcp().bind(address.getHostString, address.getPort)
      // Bind succeeded, we have a local address
      val binding1 = bind.to(Sink.fromSubscriber(probe1)).run().futureValue

      probe1.expectSubscription()

      val probe2 = TestSubscriber.manualProbe[Tcp.IncomingConnection]()
      val binding2F = bind.to(Sink.fromSubscriber(probe2)).run()
      probe2.expectSubscriptionAndError(signalDemand = true) shouldBe a[BindFailedException]

      val probe3 = TestSubscriber.manualProbe[Tcp.IncomingConnection]()
      val binding3F = bind.to(Sink.fromSubscriber(probe3)).run()
      probe3.expectSubscriptionAndError()

      binding2F.failed.futureValue shouldBe a[BindFailedException]
      binding3F.failed.futureValue shouldBe a[BindFailedException]

      // Now unbind first
      binding1.unbind().futureValue
      probe1.expectComplete()

      val probe4 = TestSubscriber.manualProbe[Tcp.IncomingConnection]()
      // Bind succeeded, we have a local address
      val binding4 = bind.to(Sink.fromSubscriber(probe4)).run().futureValue
      probe4.expectSubscription()

      // clean up
      binding4.unbind().futureValue
    }

    "not shut down connections after the connection stream cancelled" in {

      // configure a few timeouts we do not want to hit
      val config = ConfigFactory.parseString("""
        akka.actor.serializer-messages = off
        akka.io.tcp.register-timeout = 42s
        akka.stream.materializer.subscription-timeout.mode = cancel
        akka.stream.materializer.subscription-timeout.timeout = 42s
      """)
      val serverSystem = ActorSystem("server", config)
      val clientSystem = ActorSystem("client", config)

      try {

        val address = temporaryServerAddress()
        val completeRequest = TestLatch()(serverSystem)
        val serverGotRequest = Promise[Done]()

        def portClosed(): Boolean =
          try {
            val socket = new Socket()
            socket.connect(address, 250)
            socket.close()
            serverSystem.log.info("port open")
            false
          } catch {
            case _: SocketTimeoutException => true
            case _: SocketException        => true
          }

        import serverSystem.dispatcher
        val futureBinding: Future[ServerBinding] =
          Tcp(serverSystem)
            .bind(address.getHostString, address.getPort)
            // accept one connection, then cancel
            .take(1)
            // keep the accepted request hanging
            .map { connection =>
              serverGotRequest.success(Done)
              Future {
                Await.ready(completeRequest, remainingOrDefault) // wait for the port close below
                // when the server has closed the port and stopped accepting incoming
                // connections, complete the one accepted connection
                connection.flow.join(Flow[ByteString]).run()
              }
            }
            .to(Sink.ignore)
            .run()(SystemMaterializer(serverSystem).materializer)

        // make sure server is running first
        futureBinding.futureValue

        // then connect once, which should lead to the server cancelling
        val total = Source(immutable.Iterable.fill(100)(ByteString(0)))
          .via(Tcp(clientSystem).outgoingConnection(address))
          .runFold(0)(_ + _.size)(SystemMaterializer(clientSystem).materializer)

        serverGotRequest.future.futureValue
        // this can take a bit of time worst case but is often swift
        awaitCond(portClosed())
        completeRequest.open()

        total.futureValue should ===(100) // connection

      } finally {
        TestKit.shutdownActorSystem(serverSystem)
        TestKit.shutdownActorSystem(clientSystem)
      }
    }

    "handle single connection when connection flow is immediately cancelled" in {
      implicit val ec: ExecutionContext = system.dispatcher

      val (bindingFuture, connection) = Tcp().bind("localhost", 0).toMat(Sink.head)(Keep.both).run()

      connection.map { c =>
        c.handleWith(Flow[ByteString])
      }

      val binding = bindingFuture.futureValue

      val expected = ByteString("test")
      val msg = Source.single(expected).via(Tcp().outgoingConnection(binding.localAddress)).runWith(Sink.head)
      msg.futureValue shouldBe expected

      binding.unbind()
    }

    "shut down properly even if some accepted connection Flows have not been subscribed to" in {
      val address = temporaryServerAddress()
      val firstClientConnected = Promise[Unit]()
      val secondClientIgnored = Promise[Unit]()
      val connectionCounter = new AtomicInteger(0)

      val accept2ConnectionSink: Sink[IncomingConnection, NotUsed] =
        Flow[IncomingConnection]
          .take(2)
          .mapAsync(2) { incoming =>
            val connectionNr = connectionCounter.incrementAndGet()
            if (connectionNr == 1) {
              // echo
              incoming.flow
                .joinMat(Flow[ByteString]
                  .mapMaterializedValue { mat =>
                    firstClientConnected.trySuccess(())
                    mat
                  }
                  .watchTermination()(Keep.right))(Keep.right)
                .run()
            } else {
              // just ignore it
              secondClientIgnored.trySuccess(())
              Future.successful(Done)
            }
          }
          .to(Sink.ignore)

      val serverBound =
        Tcp().bind(address.getHostString, address.getPort).toMat(accept2ConnectionSink)(Keep.left).run()

      // make sure server has started
      serverBound.futureValue

      val firstProbe = TestPublisher.probe[ByteString]()
      val firstResult = Source.fromPublisher(firstProbe).via(Tcp().outgoingConnection(address)).runWith(Sink.seq)

      // create the first connection and wait until the flow is running server side
      firstClientConnected.future.futureValue(Timeout(5.seconds))
      firstProbe.expectRequest()
      firstProbe.sendNext(ByteString(23))

      // then connect the second one, which will be ignored
      val rejected = Source(List(ByteString(67))).via(Tcp().outgoingConnection(address)).runWith(Sink.seq)
      secondClientIgnored.future.futureValue

      // first connection should be fine
      firstProbe.sendComplete()
      firstResult.futureValue(Timeout(10.seconds)) should ===(Seq(ByteString(23)))

      // as the second server connection was never connected to it will be failed
      rejected.failed.futureValue(Timeout(5.seconds)) shouldBe a[StreamTcpException]
    }

    "not thrown on unbind after system has been shut down" in {
      val sys2 = ActorSystem("shutdown-test-system")
      implicit val materializer = SystemMaterializer(sys2).materializer
      try {
        val address = temporaryServerAddress()

        val bindingFuture = Tcp().bindAndHandle(Flow[ByteString], address.getHostString, address.getPort)

        // Ensure server is running
        bindingFuture.futureValue
        // and is possible to communicate with
        Source.single(ByteString(0)).via(Tcp().outgoingConnection(address)).runWith(Sink.ignore).futureValue

        sys2.terminate().futureValue

        val binding = bindingFuture.futureValue
        binding.unbind().futureValue
      } finally sys2.terminate()
    }

    "show host and port in bind exception message" in EventFilter[BindException](occurrences = 1).intercept {
      val (host, port) = temporaryServerHostnameAndPort()
      val bind = Tcp().bind(host, port)

      val probe1 = TestSubscriber.manualProbe[Tcp.IncomingConnection]()
      val binding1 = bind.to(Sink.fromSubscriber(probe1)).run().futureValue

      probe1.expectSubscription()

      val probe2 = TestSubscriber.manualProbe[Tcp.IncomingConnection]()
      val binding2 = bind.to(Sink.fromSubscriber(probe2)).run()

      val thrown = the[BindFailedException] thrownBy Await.result(binding2, 3.seconds)
      thrown.getMessage should include(host)
      thrown.getMessage should include(port.toString)

      // clean up
      binding1.unbind().futureValue
    }
  }

  "TLS client and server convenience methods with SSLEngine setup" should {

    "allow for TLS" in {
      // cert is valid until 2025, so if this tests starts failing after that you need to create a new one
      val address = temporaryServerAddress()

      Tcp()
        .bindAndHandleWithTls(
          // just echo characters until we reach '\n', then complete stream
          // also - byte is our framing
          Flow[ByteString].mapConcat(_.utf8String.toList).takeWhile(_ != '\n').map(c => ByteString(c)),
          address.getHostName,
          address.getPort,
          () => createSSLEngine(TLSRole.server))
        .futureValue
      system.log.info(s"Server bound to ${address.getHostString}:${address.getPort}")

      val connectionFlow =
        Tcp().outgoingConnectionWithTls(address, () => createSSLEngine(TLSRole.client))

      val chars = "hello\n".toList.map(_.toString)
      val (connectionF, result) =
        Source(chars)
          .map(c => ByteString(c))
          .concat(Source.maybe) // do not complete it from our side
          .viaMat(connectionFlow)(Keep.right)
          .map(_.utf8String)
          .toMat(Sink.fold("")(_ + _))(Keep.both)
          .run()

      connectionF.futureValue
      system.log.info(s"Client connected to ${address.getHostString}:${address.getPort}")

      result.futureValue(PatienceConfiguration.Timeout(10.seconds)) should ===("hello")
    }

    // #setting-up-ssl-engine
    import java.security.KeyStore
    import javax.net.ssl.KeyManagerFactory
    import javax.net.ssl.SSLContext
    import javax.net.ssl.SSLEngine
    import javax.net.ssl.TrustManagerFactory

    import akka.stream.TLSRole

    // initialize SSLContext once
    lazy val sslContext: SSLContext = {
      // Don't hardcode your password in actual code
      val password = "abcdef".toCharArray

      // trust store and keys in one keystore
      val keyStore = KeyStore.getInstance("PKCS12")
      keyStore.load(getClass.getResourceAsStream("/tcp-spec-keystore.p12"), password)

      val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
      trustManagerFactory.init(keyStore)

      val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
      keyManagerFactory.init(keyStore, password)

      // init ssl context
      val context = SSLContext.getInstance("TLSv1.2")
      context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
      context
    }

    // create new SSLEngine from the SSLContext, which was initialized once
    def createSSLEngine(role: TLSRole): SSLEngine = {
      val engine = sslContext.createSSLEngine()

      engine.setUseClientMode(role == akka.stream.Client)
      engine.setEnabledCipherSuites(Array("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"))
      engine.setEnabledProtocols(Array("TLSv1.2"))

      engine
    }
    // #setting-up-ssl-engine

  }

  def validateServerClientCommunication(
      testData: ByteString,
      serverConnection: ServerConnection,
      readProbe: TcpReadProbe,
      writeProbe: TcpWriteProbe): Unit = {
    serverConnection.write(testData)
    serverConnection.read(5)
    readProbe.read(5) should be(testData)
    writeProbe.write(testData)
    serverConnection.waitRead() should be(testData)
  }
}
