/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.util.control.NoStackTrace

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor._
import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.remote.{ RARP, RemotingMultiNodeSpec }
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object SurviveInboundStreamRestartWithCompressionInFlightSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
        akka.loglevel = INFO
        akka.remote.artery {
          advanced {
            inbound-lanes = 4
            give-up-system-message-after = 4s
            compression.actor-refs.advertisement-interval = 300ms
            compression.manifests.advertisement-interval = 1 minute
          }
        }
        """))
      .withFallback(RemotingMultiNodeSpec.commonConfig))

  testTransport(on = true)

  /**
   * ForwardActor tells all messages as-is to specified ActorRef.
   *
   * @param ref target ActorRef to forward messages to
   */
  case class TellAndEcho(ref: ActorRef) extends Actor {
    override def receive = {
      case msg =>
        ref ! msg
        val reply = s"${self.path.name}-$msg"
        sender() ! reply
    }
  }

  object TellAndEcho {
    def props(ref: ActorRef) = Props(TellAndEcho(ref))
  }

}

class SurviveInboundStreamRestartWithCompressionInFlightSpecMultiJvmNode1
    extends SurviveInboundStreamRestartWithCompressionInFlightSpec

class SurviveInboundStreamRestartWithCompressionInFlightSpecMultiJvmNode2
    extends SurviveInboundStreamRestartWithCompressionInFlightSpec

abstract class SurviveInboundStreamRestartWithCompressionInFlightSpec
    extends RemotingMultiNodeSpec(SurviveInboundStreamRestartWithCompressionInFlightSpec)
    with ImplicitSender
    with ScalaFutures {

  import SurviveInboundStreamRestartWithCompressionInFlightSpec._

  override def initialParticipants = roles.size

  "Decompression table" must {

    import scala.concurrent.duration._

    "be kept even if inbound lane is restarted, and decode into correct actors still" taggedAs LongRunningTest in {
      val probeA = TestProbe()
      val probeB = TestProbe()

      runOn(first) {
        system.actorOf(TellAndEcho.props(probeA.ref), "receiver-a")
        system.actorOf(TellAndEcho.props(probeB.ref), "receiver-b")
      }
      info("receivers-started")
      enterBarrier("receivers-started")

      // we'll be sending to first from second, but easier to obtain the refs up-front like this
      system.actorSelection(node(first) / "user" / "receiver-a") ! Identify("a")
      val sendToA = expectMsgType[ActorIdentity].ref.get

      system.actorSelection(node(first) / "user" / "receiver-b") ! Identify("b")
      val sendToB = expectMsgType[ActorIdentity].ref.get

      runOn(second) {
        (1 to 100).foreach { i =>
          pingPong(sendToA, s"a$i")
        }
        info("done sending to A, first round")

        (1 to 100).foreach { i =>
          pingPong(sendToB, s"a$i")
        }
        info("done sending to B, first round")
      }
      enterBarrier("sender-started")

      Thread.sleep(2000) // table should propagate...
      // expected table state: 0:deadLetters, 1:a, 2:b
      enterBarrier("done sleeping between rounds")

      runOn(first) {
        // cause inbound stream restart:
        val ex = new Exception("Inbound stream failed. Boom!") with NoStackTrace
        val failCommand = TestManagementCommands.FailInboundStreamOnce(ex)
        RARP(system).provider.transport.asInstanceOf[ArteryTransport].managementCommand(failCommand).futureValue
      }
      enterBarrier("inbound-failure-restart-first")

      runOn(second) {
        sendToB.tell("trigger", ActorRef.noSender)
        // when using inbound-lanes > 1 we can't be sure when it's done, another message (e.g. HandshakeReq)
        // might have triggered the restart
        Thread.sleep(2000)

        // we poke the remote system, awaiting its inbound stream recovery, then it should reply
        awaitAssert({
          sendToB ! "alive-again"
          expectMsg(300.millis, s"${sendToB.path.name}-alive-again")
        }, max = 5.seconds, interval = 500.millis)

        // we continue sending messages using the "old table".
        // if a new table was being built, it would cause the b to be compressed as 1 causing a wrong reply to come back
        (1 to 100).foreach { i =>
          pingPong(sendToB, s"b$i")
        }
        (1 to 100).foreach { i =>
          pingPong(sendToA, s"a$i")
        }

        info("received correct replies from restarted system!")
      }

      enterBarrier("done")
    }

  }

  private def pingPong(target: ActorRef, msg: String) = {
    target ! msg
    // expect the echo response, with prefixed actual recipient name
    // this is to verify the recipient is indeed the one we intended to send to.
    // this tests that the table never gets "mixed up"
    expectMsg(s"${target.path.name}-$msg")
  }
}
