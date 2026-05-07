/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter

import akka.actor.{ ActorIdentity, ActorSystem, Identify }
import akka.pattern.ask
import akka.remote.RARP
import akka.remote.artery.{ ArteryMultiNodeSpec, ArterySpecSupport, ArteryTransport }
import akka.remote.artery.compress.CompressionProtocol.Events.{ Event, ReceivedActorRefCompressionTable }
import akka.testkit._
import akka.util.Timeout

object HandshakeShouldDropCompressionTableSpec {
  val commonConfig = ConfigFactory.parseString("""
     akka {
       remote.artery.advanced.handshake-timeout = 10s
       remote.artery.advanced.aeron.image-liveness-timeout = 7s

       remote.artery.advanced.compression {
         actor-refs {
           # we'll trigger advertisement manually
           advertisement-interval = 10 hours
         }
       }
     }
  """).withFallback(ArterySpecSupport.defaultConfig)

}

class HandshakeShouldDropCompressionTableSpec
    extends ArteryMultiNodeSpec(HandshakeShouldDropCompressionTableSpec.commonConfig)
    with ImplicitSender
    with BeforeAndAfter {

  implicit val t: Timeout = Timeout(3.seconds)
  var systemB: ActorSystem = null
  val portB = freePort()

  before {
    systemB = newRemoteSystem(name = Some("systemB"), extraConfig = Some(s"akka.remote.artery.canonical.port = $portB"))
  }

  "Outgoing compression table" must {
    "be dropped on system restart" in {
      val messagesToExchange = 10

      // System A transport:
      RARP(system).provider.transport.asInstanceOf[ArteryTransport]

      def systemBTransport = RARP(systemB).provider.transport.asInstanceOf[ArteryTransport]

      // listen for compression table events
      val aProbe = TestProbe()
      val a1Probe = TestProbe()
      val b1Probe = TestProbe()(systemB)
      system.eventStream.subscribe(aProbe.ref, classOf[Event])
      systemB.eventStream.subscribe(b1Probe.ref, classOf[Event])

      def echoSel = system.actorSelection(s"akka://systemB@localhost:$portB/user/echo")
      systemB.actorOf(TestActors.echoActorProps, "echo")

      // cause testActor-1 to become a heavy hitter
      (1 to messagesToExchange).foreach { i =>
        echoSel ! s"hello-$i"
      } // does not reply, but a hot receiver should be advertised
      waitForEcho(this, s"hello-$messagesToExchange")
      systemBTransport.triggerCompressionAdvertisements(actorRef = true, manifest = false)

      val a0 = aProbe.expectMsgType[ReceivedActorRefCompressionTable](10.seconds)
      info("System [A] received: " + a0)
      a0.table.dictionary.keySet should contain(testActor)

      // cause a1Probe to become a heavy hitter (we want to not have it in the 2nd compression table later)
      (1 to messagesToExchange).foreach { i =>
        echoSel.tell(s"hello-$i", a1Probe.ref)
      }
      waitForEcho(a1Probe, s"hello-$messagesToExchange")
      systemBTransport.triggerCompressionAdvertisements(actorRef = true, manifest = false)

      val a1 = aProbe.expectMsgType[ReceivedActorRefCompressionTable](10.seconds)
      info("System [A] received: " + a1)
      a1.table.dictionary.keySet should contain(a1Probe.ref)

      log.info("SHUTTING DOWN system {}...", systemB)
      shutdown(systemB)
      systemB =
        newRemoteSystem(name = Some("systemB"), extraConfig = Some(s"akka.remote.artery.canonical.port = $portB"))
      Thread.sleep(1000)
      log.info("SYSTEM READY {}...", systemB)

      val aNewProbe = TestProbe()
      system.eventStream.subscribe(aNewProbe.ref, classOf[Event])

      systemB.actorOf(TestActors.echoActorProps, "echo") // start it again
      (1 to 5).foreach { _ =>
        // since some messages may end up being lost
        (1 to messagesToExchange).foreach { i =>
          echoSel ! s"hello-$i"
        } // does not reply, but a hot receiver should be advertised
        Thread.sleep(100)
      }
      waitForEcho(this, s"hello-$messagesToExchange", max = 10.seconds)
      systemBTransport.triggerCompressionAdvertisements(actorRef = true, manifest = false)

      val a2 = aNewProbe.expectMsgType[ReceivedActorRefCompressionTable](10.seconds)
      info("System [A] received: " + a2)
      a2.table.dictionary.keySet should contain(testActor)

      val aNew2Probe = TestProbe()
      (1 to messagesToExchange).foreach { i =>
        echoSel.tell(s"hello-$i", aNew2Probe.ref)
      } // does not reply, but a hot receiver should be advertised
      waitForEcho(aNew2Probe, s"hello-$messagesToExchange")
      systemBTransport.triggerCompressionAdvertisements(actorRef = true, manifest = false)

      val a3 = aNewProbe.expectMsgType[ReceivedActorRefCompressionTable](10.seconds)
      info("Received second compression: " + a3)
      a3.table.dictionary.keySet should contain(aNew2Probe.ref)
    }
  }

  def waitForEcho(probe: TestKit, m: String, max: Duration = 3.seconds): Any =
    probe.fishForMessage(max = max, hint = s"waiting for '$m'") {
      case `m` => true
      case _   => false
    }

  def identify(_system: String, port: Int, name: String) = {
    val selection =
      system.actorSelection(s"akka://${_system}@localhost:$port/user/$name")
    val identity = Await.result((selection ? Identify(1)).mapTo[ActorIdentity], 3.seconds)
    if (identity.correlationId != 1) throw new RuntimeException("Got the wrong identity back")
    identity.ref.get
  }

}
