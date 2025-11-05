/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.io.IOException
import java.net.{ InetSocketAddress, ServerSocket }

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.TimeLimits

import akka.actor.{ ActorRef, PoisonPill }
import akka.io.Tcp._
import akka.testkit.{ AkkaSpec, TestProbe }
import akka.testkit.WithLogCapturing
import akka.util.ByteString

class TcpIntegrationSpec extends AkkaSpec("""
    akka.loglevel = debug
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.io.tcp.trace-logging = on
    """) with TcpIntegrationSpecSupport with TimeLimits with WithLogCapturing {

  def verifyActorTermination(actor: ActorRef): Unit = {
    watch(actor)
    expectTerminated(actor)
  }

  "The TCP transport implementation" should {

    "properly bind a test server" in new TestSetup

    "allow connecting to and disconnecting from the test server" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      clientHandler.send(clientConnection, Close)
      clientHandler.expectMsg(Closed)
      serverHandler.expectMsg(PeerClosed)
      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort from client side" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      clientHandler.send(clientConnection, Abort)
      clientHandler.expectMsg(Aborted)
      serverHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort from client side after chit-chat" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      chitchat(clientHandler, clientConnection, serverHandler, serverConnection)

      clientHandler.send(clientConnection, Abort)
      clientHandler.expectMsg(Aborted)
      serverHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort from server side" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      serverHandler.send(serverConnection, Abort)
      serverHandler.expectMsg(Aborted)
      clientHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort from server side after chit-chat" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      chitchat(clientHandler, clientConnection, serverHandler, serverConnection)

      serverHandler.send(serverConnection, Abort)
      serverHandler.expectMsg(Aborted)
      clientHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort via PoisonPill from client side" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      clientHandler.send(clientConnection, PoisonPill)
      verifyActorTermination(clientConnection)

      serverHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort via PoisonPill from client side after chit-chat" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      chitchat(clientHandler, clientConnection, serverHandler, serverConnection)

      clientHandler.send(clientConnection, PoisonPill)
      verifyActorTermination(clientConnection)

      serverHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(serverConnection)
    }

    "properly handle connection abort via PoisonPill from server side" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      serverHandler.send(serverConnection, PoisonPill)
      verifyActorTermination(serverConnection)

      clientHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(clientConnection)
    }

    "properly handle connection abort via PoisonPill from server side after chit-chat" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
      chitchat(clientHandler, clientConnection, serverHandler, serverConnection)

      serverHandler.send(serverConnection, PoisonPill)
      verifyActorTermination(serverConnection)

      clientHandler.expectMsgType[ErrorClosed]
      verifyActorTermination(clientConnection)
    }

    "properly complete one client/server request/response cycle" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()

      object Aye extends Event
      object Yes extends Event

      clientHandler.send(clientConnection, Write(ByteString("Captain on the bridge!"), Aye))
      clientHandler.expectMsg(Aye)
      serverHandler.expectMsgType[Received].data.decodeString("ASCII") should ===("Captain on the bridge!")

      serverHandler.send(serverConnection, Write(ByteString("For the king!"), Yes))
      serverHandler.expectMsg(Yes)
      clientHandler.expectMsgType[Received].data.decodeString("ASCII") should ===("For the king!")

      serverHandler.send(serverConnection, Close)
      serverHandler.expectMsg(Closed)
      clientHandler.expectMsg(PeerClosed)

      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "support waiting for writes with backpressure" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()

      object Ack extends Event

      serverHandler.send(serverConnection, Write(ByteString(Array.fill[Byte](100000)(0)), Ack))
      serverHandler.expectMsg(Ack)

      expectReceivedData(clientHandler, 100000)

      override def bindOptions = List(SO.SendBufferSize(1024))
      override def connectOptions = List(SO.ReceiveBufferSize(1024))

      serverHandler.send(serverConnection, Close)
      serverHandler.expectMsg(Closed)
      clientHandler.expectMsg(PeerClosed)

      verifyActorTermination(clientConnection)
      verifyActorTermination(serverConnection)
    }

    "don't report Connected when endpoint isn't responding" in {
      val connectCommander = TestProbe()
      // a "random" endpoint hopefully unavailable since it's in the test-net IP range
      val endpoint = new InetSocketAddress("192.0.2.1", 23825)
      connectCommander.send(IO(Tcp), Connect(endpoint))
      // expecting CommandFailed or no reply (within timeout)
      val replies = connectCommander.receiveWhile(1.second) { case m: Connected => m }
      replies should ===(Nil)
    }

    "handle tcp connection actor death properly" in new TestSetup(shouldBindServer = false) {
      val serverSocket = new ServerSocket(endpoint.getPort(), 100, endpoint.getAddress())
      val connectCommander = TestProbe()
      connectCommander.send(IO(Tcp), Connect(endpoint))

      val accept = serverSocket.accept()
      connectCommander.expectMsgType[Connected].remoteAddress should ===(endpoint)
      val connectionActor = connectCommander.lastSender
      connectCommander.send(connectionActor, PoisonPill)
      failAfter(3 seconds) {
        try {
          accept.getInputStream.read() should ===(-1)
        } catch {
          case _: IOException => // this is also fine
        }
      }
      verifyActorTermination(connectionActor)
    }
  }

  def chitchat(
      clientHandler: TestProbe,
      clientConnection: ActorRef,
      serverHandler: TestProbe,
      serverConnection: ActorRef,
      rounds: Int = 100) = {

    val testData = ByteString(0)
    (1 to rounds).foreach { _ =>
      clientHandler.send(clientConnection, Write(testData))
      serverHandler.expectMsg(Received(testData))
      serverHandler.send(serverConnection, Write(testData))
      clientHandler.expectMsg(Received(testData))
    }
  }

}
