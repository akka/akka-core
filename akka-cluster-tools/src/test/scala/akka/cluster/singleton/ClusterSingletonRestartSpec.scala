/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.TestActors
import akka.testkit.TestProbe

class ClusterSingletonRestartSpec
    extends AkkaSpec("""
  akka.loglevel = INFO
  akka.actor.provider = akka.cluster.ClusterActorRefProvider
  akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
  akka.cluster.testkit.auto-down-unreachable-after = 2s
  akka.remote {
    artery.canonical {
      hostname = "127.0.0.1"
      port = 0
    }
  }
  """) {

  val sys1 = ActorSystem(system.name, system.settings.config)
  val sys2 = ActorSystem(system.name, system.settings.config)
  var sys3: ActorSystem = null

  def join(from: ActorSystem, to: ActorSystem): Unit = {
    from.actorOf(
      ClusterSingletonManager.props(
        singletonProps = TestActors.echoActorProps,
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(from)),
      name = "echo")

    within(10.seconds) {
      awaitAssert {
        Cluster(from).join(Cluster(to).selfAddress)
        Cluster(from).state.members.map(_.uniqueAddress) should contain(Cluster(from).selfUniqueAddress)
        Cluster(from).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
      }
    }
  }

  "Restarting cluster node with same hostname and port" must {
    "hand-over to next oldest" in {
      join(sys1, sys1)
      join(sys2, sys1)

      val proxy2 = sys2.actorOf(ClusterSingletonProxy.props("user/echo", ClusterSingletonProxySettings(sys2)), "proxy2")

      within(5.seconds) {
        awaitAssert {
          val probe = TestProbe()(sys2)
          proxy2.tell("hello", probe.ref)
          probe.expectMsg(1.second, "hello")
        }
      }

      shutdown(sys1)
      // it will be downed by the join attempts of the new incarnation

      sys3 = {
        val sys1port = Cluster(sys1).selfAddress.port.get

        val sys3Config =
          ConfigFactory.parseString(s"""
            akka.remote.artery.canonical.port=$sys1port
            """).withFallback(system.settings.config)

        ActorSystem(system.name, sys3Config)
      }
      join(sys3, sys2)

      within(5.seconds) {
        awaitAssert {
          val probe = TestProbe()(sys2)
          proxy2.tell("hello2", probe.ref)
          probe.expectMsg(1.second, "hello2")
        }
      }

      Cluster(sys2).leave(Cluster(sys2).selfAddress)

      within(15.seconds) {
        awaitAssert {
          Cluster(sys3).state.members.map(_.uniqueAddress) should ===(Set(Cluster(sys3).selfUniqueAddress))
        }
      }

      val proxy3 = sys3.actorOf(ClusterSingletonProxy.props("user/echo", ClusterSingletonProxySettings(sys3)), "proxy3")

      within(5.seconds) {
        awaitAssert {
          val probe = TestProbe()(sys3)
          proxy3.tell("hello3", probe.ref)
          probe.expectMsg(1.second, "hello3")
        }
      }

    }
  }

  override def afterTermination(): Unit = {
    shutdown(sys1)
    shutdown(sys2)
    if (sys3 != null)
      shutdown(sys3)
  }
}
