/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.DatagramSocket
import java.net.InetSocketAddress

import akka.actor.ActorRef
import akka.io.Inet._
import akka.io.Udp._
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }
import akka.testkit.SocketUtil.temporaryServerAddresses
import akka.util.ByteString

class UdpIntegrationSpec extends AkkaSpec("""
    akka.loglevel = INFO
    # tests expect to be able to mutate messages
    """) with ImplicitSender {

  def bindUdp(handler: ActorRef): InetSocketAddress = {
    val commander = TestProbe()
    commander.send(IO(Udp), Bind(handler, new InetSocketAddress("127.0.0.1", 0)))
    commander.expectMsgType[Bound].localAddress
  }

  def bindUdp(address: InetSocketAddress, handler: ActorRef): ActorRef = {
    val commander = TestProbe()
    commander.send(IO(Udp), Bind(handler, address))
    commander.expectMsg(Bound(address))
    commander.sender()
  }

  def createSimpleSender(): ActorRef = {
    val commander = TestProbe()
    commander.send(IO(Udp), SimpleSender)
    commander.expectMsg(SimpleSenderReady)
    commander.sender()
  }

  "The UDP Fire-and-Forget implementation" must {

    "be able to send without binding" in {
      val serverAddress = bindUdp(testActor)
      val data = ByteString("To infinity and beyond!")
      val simpleSender = createSimpleSender()
      simpleSender ! Send(data, serverAddress)

      expectMsgType[Received].data should ===(data)
    }

    "be able to deliver subsequent messages after address resolution failure" in {
      val unresolvableServerAddress = new InetSocketAddress("some-unresolvable-host", 10000)
      val cmd = Send(ByteString("Can't be delivered"), unresolvableServerAddress)
      val simpleSender = createSimpleSender()
      simpleSender ! cmd
      expectMsgType[CommandFailed].cmd should ===(cmd)

      val serverAddress = bindUdp(testActor)
      val data = ByteString("To infinity and beyond!")
      simpleSender ! Send(data, serverAddress)
      expectMsgType[Received].data should ===(data)
    }

    "be able to send several packet back and forth with binding" in {
      val addresses = temporaryServerAddresses(2, udp = true)
      val serverAddress = addresses(0)
      val clientAddress = addresses(1)
      val server = bindUdp(serverAddress, testActor)
      val client = bindUdp(clientAddress, testActor)
      val data = ByteString("Fly little packet!")

      def checkSendingToClient(): Unit = {
        server ! Send(data, clientAddress)
        expectMsgPF() {
          case Received(indata, s) =>
            indata should ===(data)
            s should ===(serverAddress)
        }
      }
      def checkSendingToServer(): Unit = {
        client ! Send(data, serverAddress)
        expectMsgPF() {
          case Received(indata, s) =>
            indata should ===(data)
            s should ===(clientAddress)
        }
      }

      (0 until 20).foreach(_ => checkSendingToServer())
      (0 until 20).foreach(_ => checkSendingToClient())
      (0 until 20).foreach { i =>
        if (i % 2 == 0) checkSendingToServer()
        else checkSendingToClient()
      }
    }

    "call SocketOption.beforeBind method before bind." in {
      val commander = TestProbe()
      val assertOption = AssertBeforeBind()
      commander.send(IO(Udp), Bind(testActor, new InetSocketAddress("127.0.0.1", 0), options = List(assertOption)))
      commander.expectMsgType[Bound]
      assert(assertOption.beforeCalled === 1)
    }

    "call SocketOption.afterConnect method after binding." in {
      val commander = TestProbe()
      val assertOption = AssertAfterChannelBind()
      commander.send(IO(Udp), Bind(testActor, new InetSocketAddress("127.0.0.1", 0), options = List(assertOption)))
      commander.expectMsgType[Bound]
      assert(assertOption.afterCalled === 1)
    }

    "call DatagramChannelCreator.create method when opening channel" in {
      val commander = TestProbe()
      val assertOption = AssertOpenDatagramChannel()
      commander.send(IO(Udp), Bind(testActor, new InetSocketAddress("127.0.0.1", 0), options = List(assertOption)))
      commander.expectMsgType[Bound]
      assert(assertOption.openCalled === 1)
    }
  }

}

private case class AssertBeforeBind() extends SocketOption {
  @volatile
  var beforeCalled = 0

  override def beforeDatagramBind(ds: DatagramSocket): Unit = {
    assert(!ds.isBound)
    beforeCalled += 1
  }
}

private case class AssertAfterChannelBind() extends SocketOptionV2 {
  @volatile
  var afterCalled = 0

  override def afterBind(s: DatagramSocket) = {
    assert(s.isBound)
    afterCalled += 1
  }
}

private case class AssertOpenDatagramChannel() extends DatagramChannelCreator {
  @volatile
  var openCalled = 0

  override def create() = {
    openCalled += 1
    super.create()
  }
}
