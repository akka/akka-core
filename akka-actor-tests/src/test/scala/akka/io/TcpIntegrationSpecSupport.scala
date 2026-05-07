/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext

import Tcp._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.io.Inet.SocketOption
import akka.testkit.{ AkkaSpec, TestProbe }
import akka.testkit.SocketUtil.temporaryServerAddress

trait TcpIntegrationSpecSupport { this: AkkaSpec =>

  class TestSetup(shouldBindServer: Boolean = true, runClientInExtraSystem: Boolean = true) {
    val clientSystem =
      if (runClientInExtraSystem) {
        val res = ActorSystem("TcpIntegrationSpec-client", system.settings.config)
        // terminate clientSystem after server system
        system.whenTerminated.onComplete { _ =>
          res.terminate()
        }(ExecutionContext.parasitic)
        res
      } else system
    val bindHandler = TestProbe()
    val endpoint = temporaryServerAddress()

    if (shouldBindServer) bindServer()

    def bindServer(): Unit = {
      val bindCommander = TestProbe()
      bindCommander.send(IO(Tcp), Bind(bindHandler.ref, endpoint, options = bindOptions))
      bindCommander.expectMsg(Bound(endpoint))
    }

    def establishNewClientConnection(): (TestProbe, ActorRef, TestProbe, ActorRef) = {
      val connectCommander = TestProbe()(clientSystem)
      connectCommander.send(IO(Tcp)(clientSystem), Connect(endpoint, options = connectOptions))
      val localAddress = connectCommander.expectMsgType[Connected] match {
        case Connected(`endpoint`, localAddress) => localAddress
        case Connected(other, _)                 => fail(s"No match: $other")
      }
      val clientHandler = TestProbe()(clientSystem)
      connectCommander.sender() ! Register(clientHandler.ref)

      bindHandler.expectMsgType[Connected] match {
        case Connected(`localAddress`, `endpoint`) => //ok
        case other                                 => fail(s"No match: ${other}")
      }
      val serverHandler = TestProbe()
      bindHandler.sender() ! Register(serverHandler.ref)

      (clientHandler, connectCommander.sender(), serverHandler, bindHandler.sender())
    }

    @tailrec final def expectReceivedData(handler: TestProbe, remaining: Int): Unit =
      if (remaining > 0) {
        val recv = handler.expectMsgType[Received]
        expectReceivedData(handler, remaining - recv.data.size)
      }

    /** allow overriding socket options for server side channel */
    def bindOptions: immutable.Iterable[SocketOption] = Nil

    /** allow overriding socket options for client side channel */
    def connectOptions: immutable.Iterable[SocketOption] = Nil
  }

}
