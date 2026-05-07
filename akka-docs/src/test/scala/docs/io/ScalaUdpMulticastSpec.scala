/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.io

import java.net.Inet6Address
import java.net.NetworkInterface

import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.Udp
import akka.testkit.SocketUtil
import akka.testkit.TestKit
import scala.jdk.CollectionConverters._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.Random

class ScalaUdpMulticastSpec
    extends TestKit(ActorSystem("ScalaUdpMulticastSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  "listener" should {
    "send message back to sink" in {
      val ipv6ifaces =
        NetworkInterface.getNetworkInterfaces.asScala.toSeq.filter(
          iface =>
            iface.supportsMulticast &&
            iface.isUp &&
            iface.getInetAddresses.asScala.exists(_.isInstanceOf[Inet6Address]))

      if (ipv6ifaces.isEmpty) {
        // IPv6 not supported for any interface on this platform
        pending
      } else {
        // lots of problems with choosing the wrong interface for this test depending
        // on the platform (awsdl0 can't be used on OSX, docker[0-9] can't be used in a docker machine etc.)
        // therefore: try hard to find an interface that _does_ work, and only fail if there was any potentially
        // working interfaces but all failed
        var failures: List[AssertionError] = Nil
        var foundOneThatWorked = false
        val iterator = ipv6ifaces.iterator
        while (!foundOneThatWorked && iterator.hasNext) {
          val ipv6iface = iterator.next()
          // host assigned link local multicast address https://www.rfc-editor.org/rfc/rfc3307#section-4.3.2
          // generate a random 32 bit multicast address with the high order bit set
          val randomAddress: String = (Random.nextInt().abs.toLong | (1L << 31)).toHexString.toUpperCase
          val group = randomAddress.grouped(4).mkString("FF02::", ":", "")
          val port = SocketUtil.temporaryUdpIpv6Port(ipv6iface)
          val msg = "ohi"
          val sink = testActor
          val iface = ipv6iface.getName
          val listener = system.actorOf(Props(classOf[Listener], iface, group, port, sink))
          try {
            expectMsgType[Udp.Bound]
            system.actorOf(Props(classOf[Sender], iface, group, port, msg))
            // fails here, so binding succeeds but sending a message does not
            expectMsg(msg)
            foundOneThatWorked = true

          } catch {
            case ex: AssertionError =>
              system.log.info("Failed to run test on interface {}", ipv6iface.getDisplayName)
              failures = ex :: failures

          } finally {
            // unbind
            system.stop(listener)
          }
        }

        if (failures.size == ipv6ifaces.size)
          fail(s"Multicast failed on all available interfaces: ${failures}")
      }

    }
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

}
