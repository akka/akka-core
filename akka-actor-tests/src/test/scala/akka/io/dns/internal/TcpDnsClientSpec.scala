/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.InetSocketAddress
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.io.Tcp
import akka.io.Tcp.{ Connected, PeerClosed, Register }
import akka.io.dns.{ RecordClass, RecordType }
import akka.io.dns.internal.DnsClient.Answer
import akka.testkit.EventFilter
import akka.testkit.WithLogCapturing
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }

class TcpDnsClientSpec extends AkkaSpec("""akka.loglevel = DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]""") with ImplicitSender with WithLogCapturing {
  import TcpDnsClient._

  "The async TCP DNS client" should {
    val exampleRequestMessage =
      Message(42, MessageFlags(), questions = Seq(Question("akka.io", RecordType.A, RecordClass.IN)))
    val exampleResponseMessage = Message(42, MessageFlags(answer = true))
    val dnsServerAddress = InetSocketAddress.createUnresolved("foo", 53)
    val localAddress = InetSocketAddress.createUnresolved("localhost", 13441)

    "reconnect when the server closes the connection" in {
      val tcpExtensionProbe = TestProbe()
      val answerProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      client ! exampleRequestMessage

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      expectMsgType[Tcp.Write]
      registered ! Tcp.Received(encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.write())

      answerProbe.expectMsg(Answer(42, Nil))

      // When a new request arrived after the connection is closed
      registered ! PeerClosed
      client ! exampleRequestMessage

      // Expect a reconnect
      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
    }

    "terminated if the connection terminates unexpectedly" in {
      val tcpExtensionProbe = TestProbe()
      val answerProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      client ! exampleRequestMessage

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      expectMsgType[Tcp.Write]

      answerProbe.watch(client)

      registered ! PoisonPill
      answerProbe.expectTerminated(client)
    }

    "accept a fragmented TCP response" in {
      val tcpExtensionProbe = TestProbe()
      val answerProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      client ! exampleRequestMessage

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      expectMsgType[Tcp.Write]
      val fullResponse = encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.write()
      registered ! Tcp.Received(fullResponse.take(8))
      registered ! Tcp.Received(fullResponse.drop(8))

      answerProbe.expectMsg(Answer(42, Nil))
    }

    "accept multiple fragmented TCP responses" in {
      val tcpExtensionProbe = TestProbe()
      val answerProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      client ! exampleRequestMessage

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      // pretend write+ack+write happened, so three requests written, now both coming back.
      // (we need to make sure buffer is not reordered by sandwitched responses)
      expectMsgType[Tcp.Write]
      val fullResponse1 = encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.write()
      val exampleResponseMessage2 = exampleResponseMessage.copy(id = 43)
      val fullResponse2 = encodeLength(exampleResponseMessage2.write().length) ++ exampleResponseMessage2.write()
      val exampleResponseMessage3 = exampleResponseMessage.copy(id = 44)
      val fullResponse3 = encodeLength(exampleResponseMessage3.write().length) ++ exampleResponseMessage3.write()
      registered ! Tcp.Received(fullResponse1.take(8))
      Thread.sleep(30) // give things some time to go wrong
      registered ! Tcp.Received(fullResponse1.drop(8) ++ fullResponse2.take(8))
      Thread.sleep(30)
      registered ! Tcp.Received(fullResponse2.drop(8) ++ fullResponse3.take(8))
      Thread.sleep(30)
      registered ! Tcp.Received(fullResponse3.drop(8))

      answerProbe.expectMsg(Answer(42, Nil))
      answerProbe.expectMsg(Answer(43, Nil))
      answerProbe.expectMsg(Answer(44, Nil))
    }

    "respect backpressure from the TCP actor" in {
      val tcpExtensionProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, ActorRef.noSender)))

      client ! exampleRequestMessage
      client ! exampleRequestMessage.copy(id = 43)

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      val ack = expectMsgType[Tcp.Write].ack
      expectNoMessage()
      registered ! ack

      expectMsgType[Tcp.Write]
    }

    "accept merged TCP responses" in {
      val tcpExtensionProbe = TestProbe()
      val answerProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      client ! exampleRequestMessage
      client ! exampleRequestMessage.copy(id = 43)

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      var ack = expectMsgType[Tcp.Write].ack

      registered ! ack

      ack = expectMsgType[Tcp.Write].ack

      val fullResponse =
        encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.write() ++
        encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.copy(id = 43).write()
      registered ! Tcp.Received(fullResponse.take(8))
      registered ! Tcp.Received(fullResponse.drop(8))

      answerProbe.expectMsg(Answer(42, Nil))
      answerProbe.expectMsg(Answer(43, Nil))
    }

    "report its failure to the outer client" in {
      val tcpExtensionProbe = TestProbe()
      val answerProbe = TestProbe()

      val failToConnectClient =
        system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      failToConnectClient ! exampleRequestMessage

      val connect = tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))

      failToConnectClient ! Tcp.CommandFailed(connect)

      answerProbe.expectMsg(DnsClient.TcpDropped)

      val closesWithErrorClient =
        system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, answerProbe.ref)))

      closesWithErrorClient ! exampleRequestMessage

      tcpExtensionProbe.expectMsg(connect)
      tcpExtensionProbe.lastSender ! Connected(dnsServerAddress, localAddress)
      expectMsgType[Register]
      val registered = tcpExtensionProbe.lastSender

      registered ! Tcp.ErrorClosed("BOOM!")

      answerProbe.expectMsg(DnsClient.TcpDropped)
    }

    "should drop older requests when TCP connection backpressures" in {
      val tcpExtensionProbe = TestProbe()
      val connectionProbe = TestProbe()

      val client = system.actorOf(Props(new TcpDnsClient(tcpExtensionProbe.ref, dnsServerAddress, ActorRef.noSender)))

      // initial request
      val initialRequest = exampleRequestMessage.copy(id = 1)
      client ! initialRequest

      tcpExtensionProbe.expectMsg(Tcp.Connect(dnsServerAddress))
      tcpExtensionProbe.lastSender.tell(Connected(dnsServerAddress, localAddress), connectionProbe.ref)
      connectionProbe.expectMsgType[Register]
      val registered = connectionProbe.lastSender

      var write = connectionProbe.expectMsgType[Tcp.Write]
      write.data.drop(2) shouldBe initialRequest.write()

      // 1 in flight, 14 more, buffer fits 10, should drop 5 oldest (id 2 - 6)
      EventFilter.warning(occurrences = 5, pattern = "Dropping oldest buffered DNS request").intercept {
        (2 to 16).foreach { i =>
          client ! exampleRequestMessage.copy(id = i.toShort)
        }
      }

      // initial write is acked
      registered ! write.ack

      // the rest of the buffered should be handled
      (7 to 16).foreach { i =>
        write = connectionProbe.expectMsgType[Tcp.Write]
        write.data.drop(2) shouldBe exampleRequestMessage.copy(id = i.toShort).write()
        registered ! write.ack // next ack
      }
    }
  }
}
