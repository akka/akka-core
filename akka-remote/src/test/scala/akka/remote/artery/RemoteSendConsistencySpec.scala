/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.UUID

import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Deploy
import akka.actor.Identify
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.RootActorPath
import akka.testkit.{ ImplicitSender, TestActors, TestProbe }

class ArteryUpdSendConsistencyWithOneLaneSpec
    extends AbstractRemoteSendConsistencySpec(ConfigFactory.parseString("""
      akka.remote.artery.transport = aeron-udp
      akka.remote.artery.advanced.outbound-lanes = 1
      akka.remote.artery.advanced.inbound-lanes = 1
    """).withFallback(ArterySpecSupport.defaultConfig))

class ArteryUpdSendConsistencyWithThreeLanesSpec
    extends AbstractRemoteSendConsistencySpec(ConfigFactory.parseString("""
      akka.loglevel = DEBUG
      akka.remote.artery.transport = aeron-udp
      akka.remote.artery.advanced.outbound-lanes = 3
      akka.remote.artery.advanced.inbound-lanes = 3
    """).withFallback(ArterySpecSupport.defaultConfig))

class ArteryTcpSendConsistencyWithOneLaneSpec
    extends AbstractRemoteSendConsistencySpec(ConfigFactory.parseString("""
      akka.remote.artery.transport = tcp
      akka.remote.artery.advanced.outbound-lanes = 1
      akka.remote.artery.advanced.inbound-lanes = 1
    """).withFallback(ArterySpecSupport.defaultConfig))

class ArteryTcpSendConsistencyWithThreeLanesSpec
    extends AbstractRemoteSendConsistencySpec(ConfigFactory.parseString("""
      akka.remote.artery.transport = tcp
      akka.remote.artery.advanced.outbound-lanes = 3
      akka.remote.artery.advanced.inbound-lanes = 3
    """).withFallback(ArterySpecSupport.defaultConfig))

class ArteryTlsTcpSendConsistencyWithOneLaneSpec
    extends AbstractRemoteSendConsistencySpec(ConfigFactory.parseString("""
      akka.remote.artery.transport = tls-tcp
      akka.remote.artery.advanced.outbound-lanes = 1
      akka.remote.artery.advanced.inbound-lanes = 1
    """).withFallback(ArterySpecSupport.defaultConfig))

class ArteryTlsTcpSendConsistencyWithThreeLanesSpec
    extends AbstractRemoteSendConsistencySpec(ConfigFactory.parseString("""
      akka.remote.artery.transport = tls-tcp
      akka.remote.artery.advanced.outbound-lanes = 1
      akka.remote.artery.advanced.inbound-lanes = 1
    """).withFallback(ArterySpecSupport.defaultConfig))

abstract class AbstractRemoteSendConsistencySpec(config: Config)
    extends ArteryMultiNodeSpec(config)
    with ImplicitSender {

  val systemB = newRemoteSystem(name = Some("systemB"))
  val addressB = address(systemB)
  val rootB = RootActorPath(addressB)

  private def actorRefBySelection(path: ActorPath) = {

    val correlationId = Some(UUID.randomUUID().toString)
    system.actorSelection(path) ! Identify(correlationId)

    val actorIdentity = expectMsgType[ActorIdentity](5.seconds)
    actorIdentity.correlationId shouldBe correlationId

    actorIdentity.ref.get
  }

  "Artery" must {

    "be able to identify a remote actor and ping it" in {
      systemB.actorOf(Props(new Actor {
        def receive = {
          case "ping" => sender() ! "pong"
        }
      }), "echo")

      val actorPath = rootB / "user" / "echo"
      val echoSel = system.actorSelection(actorPath)
      val echoRef = actorRefBySelection(actorPath)

      echoRef ! "ping"
      expectMsg("pong")

      echoRef ! "ping"
      expectMsg("pong")

      echoRef ! "ping"
      expectMsg("pong")

      // and actorSelection
      echoSel ! "ping"
      expectMsg("pong")

      echoSel ! "ping"
      expectMsg("pong")
    }

    "not send to remote re-created actor with same name" in {
      val echo = systemB.actorOf(TestActors.echoActorProps, "otherEcho1")
      echo ! 71
      expectMsg(71)
      echo ! PoisonPill
      echo ! 72
      val probe = TestProbe()(systemB)
      probe.watch(echo)
      probe.expectTerminated(echo)
      expectNoMessage(1.second)

      val echo2 = systemB.actorOf(TestActors.echoActorProps, "otherEcho1")
      echo2 ! 73
      expectMsg(73)
      // msg to old ActorRef (different uid) should not get through
      echo2.path.uid should not be (echo.path.uid)
      echo ! 74
      expectNoMessage(1.second)
    }

    "be able to send messages concurrently preserving order" in {
      systemB.actorOf(TestActors.echoActorProps, "echoA")
      systemB.actorOf(TestActors.echoActorProps, "echoB")
      systemB.actorOf(TestActors.echoActorProps, "echoC")

      val remoteRefA = actorRefBySelection(rootB / "user" / "echoA")
      val remoteRefB = actorRefBySelection(rootB / "user" / "echoB")
      val remoteRefC = actorRefBySelection(rootB / "user" / "echoC")

      def senderProps(remoteRef: ActorRef) =
        Props(new Actor {
          var counter = 1000
          remoteRef ! counter

          override def receive: Receive = {
            case i: Int =>
              if (i != counter) testActor ! s"Failed, expected $counter got $i"
              else if (counter == 0) {
                testActor ! "success"
                context.stop(self)
              } else {
                counter -= 1
                remoteRef ! counter
              }
          }
        }).withDeploy(Deploy.local)

      system.actorOf(senderProps(remoteRefA))
      system.actorOf(senderProps(remoteRefB))
      system.actorOf(senderProps(remoteRefC))
      system.actorOf(senderProps(remoteRefA))

      within(10.seconds) {
        expectMsg("success")
        expectMsg("success")
        expectMsg("success")
        expectMsg("success")
      }
    }

    "be able to send messages with actorSelection concurrently preserving order" in {
      systemB.actorOf(TestActors.echoActorProps, "echoA2")
      systemB.actorOf(TestActors.echoActorProps, "echoB2")
      systemB.actorOf(TestActors.echoActorProps, "echoC2")

      val selA = system.actorSelection(rootB / "user" / "echoA2")
      val selB = system.actorSelection(rootB / "user" / "echoB2")
      val selC = system.actorSelection(rootB / "user" / "echoC2")

      def senderProps(sel: ActorSelection) =
        Props(new Actor {
          var counter = 1000
          sel ! counter

          override def receive: Receive = {
            case i: Int =>
              if (i != counter) testActor ! s"Failed, expected $counter got $i"
              else if (counter == 0) {
                testActor ! "success2"
                context.stop(self)
              } else {
                counter -= 1
                sel ! counter
              }
          }
        }).withDeploy(Deploy.local)

      system.actorOf(senderProps(selA))
      system.actorOf(senderProps(selB))
      system.actorOf(senderProps(selC))
      system.actorOf(senderProps(selA))

      within(10.seconds) {
        expectMsg("success2")
        expectMsg("success2")
        expectMsg("success2")
        expectMsg("success2")
      }
    }

  }

}
