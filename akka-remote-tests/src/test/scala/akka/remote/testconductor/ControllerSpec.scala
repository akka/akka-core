/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testconductor

import java.net.InetAddress
import java.net.InetSocketAddress

import akka.actor.{ AddressFromURIString, PoisonPill, Props }
import akka.remote.testconductor.Controller.NodeInfo
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender

object ControllerSpec {
  val config = """
    akka.testconductor.barrier-timeout = 5s
    akka.actor.provider = remote
    akka.actor.debug.fsm = on
    akka.actor.debug.lifecycle = on
    """
}

class ControllerSpec extends AkkaSpec(ControllerSpec.config) with ImplicitSender {

  val A = RoleName("a")
  val B = RoleName("b")

  "A Controller" must {

    "publish its nodes" in {
      val c = system.actorOf(Props(classOf[Controller], 1, new InetSocketAddress(InetAddress.getLocalHost, 0)))
      c ! NodeInfo(A, AddressFromURIString("akka://sys"), testActor)
      expectMsg(ToClient(Done))
      c ! NodeInfo(B, AddressFromURIString("akka://sys"), testActor)
      expectMsg(ToClient(Done))
      c ! Controller.GetNodes
      expectMsgType[Iterable[RoleName]].toSet should ===(Set(A, B))
      c ! PoisonPill // clean up so network connections don't accumulate during test run
    }

  }

}
