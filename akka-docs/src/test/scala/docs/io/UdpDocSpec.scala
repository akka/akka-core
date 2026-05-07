/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.io

import akka.testkit.AkkaSpec
import akka.actor.Actor
import akka.io.IO
import akka.io.Udp
import akka.actor.ActorRef

import java.net.InetSocketAddress
import akka.util.ByteString
import akka.testkit.TestProbe
import akka.actor.Props

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.io.UdpConnected

import scala.annotation.nowarn

object ScalaUdpDocSpec {

  //#sender
  class SimpleSender(remote: InetSocketAddress) extends Actor {
    import context.system
    IO(Udp) ! Udp.SimpleSender

    def receive = {
      case Udp.SimpleSenderReady =>
        context.become(ready(sender()))
        //#sender
        sender() ! Udp.Send(ByteString("hello"), remote)
      //#sender
    }

    def ready(send: ActorRef): Receive = {
      case msg: String =>
        send ! Udp.Send(ByteString(msg), remote)
        //#sender
        if (msg == "world") send ! PoisonPill
      //#sender
    }
  }
  //#sender

  //#listener
  class Listener(nextActor: ActorRef) extends Actor {
    import context.system
    IO(Udp) ! Udp.Bind(self, new InetSocketAddress("localhost", 0))

    def receive = {
      case Udp.Bound(local) =>
        //#listener
        nextActor.forward(local)
        //#listener
        context.become(ready(sender()))
    }

    def ready(socket: ActorRef): Receive = {
      case Udp.Received(data, remote) =>
        val processed = // parse data etc., e.g. using PipelineStage
          //#listener
          data.utf8String
        //#listener
        socket ! Udp.Send(data, remote) // example server echoes back
        nextActor ! processed
      case Udp.Unbind  => socket ! Udp.Unbind
      case Udp.Unbound => context.stop(self)
    }
  }
  //#listener

  //#connected
  class Connected(remote: InetSocketAddress) extends Actor {
    import context.system
    IO(UdpConnected) ! UdpConnected.Connect(self, remote)

    def receive = {
      case UdpConnected.Connected =>
        context.become(ready(sender()))
        //#connected
        sender() ! UdpConnected.Send(ByteString("hello"))
      //#connected
    }

    def ready(connection: ActorRef): Receive = {
      case UdpConnected.Received(data) =>
        // process data, send it on, etc.
        //#connected
        if (data.utf8String == "hello")
          connection ! UdpConnected.Send(ByteString("world"))
      //#connected
      case msg: String =>
        connection ! UdpConnected.Send(ByteString(msg))
      case UdpConnected.Disconnect =>
        connection ! UdpConnected.Disconnect
      case UdpConnected.Disconnected => context.stop(self)
    }
  }
  //#connected

}

@nowarn("msg=never used") // sample snippets
abstract class UdpDocSpec extends AkkaSpec {

  def listenerProps(next: ActorRef): Props
  def simpleSenderProps(remote: InetSocketAddress): Props
  def connectedProps(remote: InetSocketAddress): Props

  "demonstrate Udp" in {
    val probe = TestProbe()
    val listen = watch(system.actorOf(listenerProps(probe.ref)))
    val local = probe.expectMsgType[InetSocketAddress]
    val listener = probe.lastSender
    val send = system.actorOf(simpleSenderProps(local))
    probe.expectMsg("hello")
    send ! "world"
    probe.expectMsg("world")
    listen ! Udp.Unbind
    expectTerminated(listen)
  }

  "demonstrate Udp suspend reading" in {
    val probe = TestProbe()
    val listen = watch(system.actorOf(listenerProps(probe.ref)))
    val local = probe.expectMsgType[InetSocketAddress]
    val listener = probe.lastSender
    listener ! Udp.SuspendReading
    Thread.sleep(1000) // no way to find out when the above is finished
    val send = system.actorOf(simpleSenderProps(local))
    probe.expectNoMessage(500.millis)
    listener ! Udp.ResumeReading
    probe.expectMsg("hello")
    send ! "world"
    probe.expectMsg("world")
    listen ! Udp.Unbind
    expectTerminated(listen)
  }

  "demonstrate UdpConnected" in {
    val probe = TestProbe()
    val listen = watch(system.actorOf(listenerProps(probe.ref)))
    val local = probe.expectMsgType[InetSocketAddress]
    val listener = probe.lastSender
    val conn = watch(system.actorOf(connectedProps(local)))
    probe.expectMsg("hello")
    probe.expectMsg("world")
    conn ! "hello"
    probe.expectMsg("hello")
    probe.expectMsg("world")
    listen ! Udp.Unbind
    expectTerminated(listen)
    conn ! UdpConnected.Disconnect
    expectTerminated(conn)
  }

}

class ScalaUdpDocSpec extends UdpDocSpec {
  import ScalaUdpDocSpec._

  override def listenerProps(next: ActorRef) = Props(new Listener(next))
  override def simpleSenderProps(remote: InetSocketAddress) = Props(new SimpleSender(remote))
  override def connectedProps(remote: InetSocketAddress) = Props(new Connected(remote))
}

class JavaUdpDocSpec extends UdpDocSpec {
  import jdocs.io.UdpDocTest._

  override def listenerProps(next: ActorRef) = Props(new Listener(next))
  override def simpleSenderProps(remote: InetSocketAddress) = Props(new SimpleSender(remote))
  override def connectedProps(remote: InetSocketAddress) = Props(new Connected(remote))
}
