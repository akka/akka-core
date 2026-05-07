/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.io

import akka.actor.{ Actor, ActorRef, Props }
import akka.io.Tcp._
import akka.io.{ IO, Tcp }

import java.net.InetSocketAddress
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }
import akka.util.ByteString

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration.Duration

@nowarn("msg=never used") // sample snippets
object PullReadingExample {

  class Listener(monitor: ActorRef) extends Actor {

    import context.system

    override def preStart(): Unit =
      //#pull-mode-bind
      IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 0), pullMode = true)
    //#pull-mode-bind

    def receive = {
      //#pull-accepting
      case Bound(localAddress) =>
        // Accept connections one by one
        sender() ! ResumeAccepting(batchSize = 1)
        context.become(listening(sender()))
        //#pull-accepting
        monitor ! localAddress
    }

    //#pull-accepting-cont
    def listening(listener: ActorRef): Receive = {
      case Connected(remote, local) =>
        val handler = context.actorOf(Props(classOf[PullEcho], sender()))
        sender() ! Register(handler, keepOpenOnPeerClosed = true)
        listener ! ResumeAccepting(batchSize = 1)
    }
    //#pull-accepting-cont

  }

  case object Ack extends Event

  class PullEcho(connection: ActorRef) extends Actor {

    //#pull-reading-echo
    override def preStart(): Unit = connection ! ResumeReading

    def receive = {
      case Received(data) => connection ! Write(data, Ack)
      case Ack            => connection ! ResumeReading
    }
    //#pull-reading-echo
  }

}

class PullReadingSpec extends AkkaSpec with ImplicitSender {

  "demonstrate pull reading" in {
    val probe = TestProbe()
    system.actorOf(Props(classOf[PullReadingExample.Listener], probe.ref), "server")
    val listenAddress = probe.expectMsgType[InetSocketAddress]

    //#pull-mode-connect
    IO(Tcp) ! Connect(listenAddress, pullMode = true)
    //#pull-mode-connect
    expectMsgType[Connected]
    val connection = lastSender

    val client = TestProbe()
    client.send(connection, Register(client.ref))
    client.send(connection, Write(ByteString("hello")))
    client.send(connection, ResumeReading)
    client.expectMsg(Received(ByteString("hello")))

    Await.ready(system.terminate(), Duration.Inf)
  }
}
