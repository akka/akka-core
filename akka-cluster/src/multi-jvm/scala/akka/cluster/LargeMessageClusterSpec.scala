/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.io.NotSerializableException

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.actor.Identify
import akka.cluster.ClusterEvent.UnreachableMember
import akka.remote.artery.ArterySettings
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.serialization.SerializerWithStringManifest
import akka.testkit._

object LargeMessageClusterMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  // Note that this test uses default configuration,
  // not MultiNodeClusterSpec.clusterConfig
  commonConfig(ConfigFactory.parseString(s"""
    akka {
      cluster.debug.verbose-heartbeat-logging = on
      loggers = ["akka.testkit.TestEventListener"]

      actor {
        provider = cluster

        serializers {
          test = "akka.cluster.LargeMessageClusterMultiJvmSpec$$SlowSerializer"
        }
        serialization-bindings {
          "akka.cluster.LargeMessageClusterMultiJvmSpec$$Slow" = test
        }
      }

      testconductor.barrier-timeout = 3 minutes

      cluster.failure-detector.acceptable-heartbeat-pause = 5 s

      remote.artery {
        enabled = on

        large-message-destinations = [ "/user/largeEcho", "/system/largeEchoProbe-3" ]

        advanced {
          maximum-large-frame-size = 2 MiB
          large-buffer-pool-size = 32
        }
      }
    }
    """))

  final case class Slow(payload: Array[Byte])

  class SlowSerializer(@nowarn("msg=never used") system: ExtendedActorSystem) extends SerializerWithStringManifest {
    override def identifier = 999
    override def manifest(o: AnyRef) = "a"
    override def toBinary(o: AnyRef) = o match {
      case Slow(payload) =>
        // simulate slow serialization to not completely overload the machine/network, see issue #24576
        Thread.sleep(100)
        payload
      case _ => throw new NotSerializableException()
    }
    override def fromBinary(bytes: Array[Byte], manifest: String) = {
      Slow(bytes)
    }
  }

}

class LargeMessageClusterMultiJvmNode1 extends LargeMessageClusterSpec
class LargeMessageClusterMultiJvmNode2 extends LargeMessageClusterSpec
class LargeMessageClusterMultiJvmNode3 extends LargeMessageClusterSpec

abstract class LargeMessageClusterSpec
    extends MultiNodeClusterSpec(LargeMessageClusterMultiJvmSpec)
    with ImplicitSender {
  import LargeMessageClusterMultiJvmSpec._

  override def expectedTestDuration: FiniteDuration = 3.minutes

  def identify(role: RoleName, actorName: String): ActorRef = within(10.seconds) {
    system.actorSelection(node(role) / "user" / actorName) ! Identify(actorName)
    expectMsgType[ActorIdentity].ref.get
  }

  val unreachableProbe = TestProbe()

  "Artery Cluster with large messages" must {

    "init cluster" taggedAs LongRunningTest in {
      Cluster(system).subscribe(unreachableProbe.ref, ClusterEvent.InitialStateAsEvents, classOf[UnreachableMember])

      awaitClusterUp(first, second, third)

      // let heartbeat monitoring begin
      unreachableProbe.expectNoMessage(5.seconds)

      enterBarrier("init-done")
    }

    "not disturb cluster heartbeat messages when sent in bursts" taggedAs LongRunningTest in {

      runOn(second, third) {
        system.actorOf(TestActors.echoActorProps, "echo")
        system.actorOf(TestActors.echoActorProps, "largeEcho")
      }
      enterBarrier("actors-started")

      runOn(second) {
        val echo3 = identify(third, "echo")
        val largeEcho3 = identify(third, "largeEcho")
        val largeEchoProbe = TestProbe(name = "largeEchoProbe")

        val largeMsgSize = 2 * 1000 * 1000
        val largeMsg = ("0" * largeMsgSize).getBytes("utf-8")
        val largeMsgBurst = 3
        val repeat = 15
        for (n <- 1 to repeat) {
          val startTime = System.nanoTime()
          for (_ <- 1 to largeMsgBurst) {
            largeEcho3.tell(largeMsg, largeEchoProbe.ref)
          }

          val ordinaryProbe = TestProbe()
          echo3.tell(("0" * 1000).getBytes("utf-8"), ordinaryProbe.ref)
          ordinaryProbe.expectMsgType[Array[Byte]]
          val ordinaryDurationMs = (System.nanoTime() - startTime) / 1000 / 1000

          largeEchoProbe.receiveN(largeMsgBurst, 20.seconds)
          println(s"Burst $n took ${(System.nanoTime() - startTime) / 1000 / 1000} ms, ordinary $ordinaryDurationMs ms")
        }
      }
      enterBarrier("sending-complete-1")

      unreachableProbe.expectNoMessage(1.seconds)

      enterBarrier("after-1")
    }

    "not disturb cluster heartbeat messages when saturated" taggedAs LongRunningTest in {

      // for non Aeron transport we use the Slow message and SlowSerializer to slow down
      // to not completely overload the machine/network, see issue #24576
      val arterySettings = ArterySettings(system.settings.config.getConfig("akka.remote.artery"))
      val aeronUdpEnabled = (arterySettings.Enabled && arterySettings.Transport == ArterySettings.AeronUpd)

      runOn(second) {
        val largeEcho2 = identify(second, "largeEcho")
        val largeEcho3 = identify(third, "largeEcho")

        val largeMsgSize = 1 * 1000 * 1000
        val payload = ("0" * largeMsgSize).getBytes("utf-8")
        val largeMsg = if (aeronUdpEnabled) payload else Slow(payload)
        (1 to 3).foreach { _ =>
          // this will ping-pong between second and third
          largeEcho2.tell(largeMsg, largeEcho3)
        }
      }

      unreachableProbe.expectNoMessage(10.seconds)

      enterBarrier("after-2")
    }
  }
}
