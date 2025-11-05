/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.InetSocketAddress

import scala.collection.immutable.Seq

import akka.actor.Props
import akka.io.Tcp
import akka.io.Tcp.{ Connected, PeerClosed, Register }
import akka.io.dns.{ RecordClass, RecordType }
import akka.io.dns.internal.DnsClient.Answer
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }

class TcpDnsClientSpec extends AkkaSpec with ImplicitSender {
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

      expectMsgType[Tcp.Write]
      expectMsgType[Tcp.Write]
      val fullResponse =
        encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.write() ++
        encodeLength(exampleResponseMessage.write().length) ++ exampleResponseMessage.copy(id = 43).write()
      registered ! Tcp.Received(fullResponse.take(8))
      registered ! Tcp.Received(fullResponse.drop(8))

      answerProbe.expectMsg(Answer(42, Nil))
      answerProbe.expectMsg(Answer(43, Nil))
    }
  }
}
