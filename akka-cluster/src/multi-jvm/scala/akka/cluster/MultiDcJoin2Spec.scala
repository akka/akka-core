/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Address
import akka.cluster.ClusterEvent.InitialStateAsEvents
import akka.cluster.ClusterEvent.MemberUp
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

// Similar to MultiDcJoinSpec, but slightly different scenario
object MultiDcJoin2MultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  nodeConfig(first, second, third)(ConfigFactory.parseString("""
    akka {
      cluster.multi-data-center.self-data-center = alpha
    }
    """))

  nodeConfig(fourth, fifth)(ConfigFactory.parseString("""
    akka {
      cluster.multi-data-center.self-data-center = beta
    }
    """))

  commonConfig(ConfigFactory.parseString("""
    akka {
      actor.provider = cluster

      loggers = ["akka.testkit.TestEventListener"]
      loglevel = INFO

      cluster {
        debug.verbose-heartbeat-logging = off
        debug.verbose-gossip-logging = off

        multi-data-center {
          cross-data-center-connections = 1
        }
      }
    }
    """))

}

class MultiDcJoin2MultiJvmNode1 extends MultiDcJoin2Spec
class MultiDcJoin2MultiJvmNode2 extends MultiDcJoin2Spec
class MultiDcJoin2MultiJvmNode3 extends MultiDcJoin2Spec
class MultiDcJoin2MultiJvmNode4 extends MultiDcJoin2Spec
class MultiDcJoin2MultiJvmNode5 extends MultiDcJoin2Spec

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiDcJoin2Spec extends MultiNodeClusterSpec(MultiDcJoin2MultiJvmSpec) {
  import MultiDcJoin2MultiJvmSpec._

  "Joining a multi-dc cluster, scenario 2" must {
    "make sure oldest is selected correctly" taggedAs LongRunningTest in {

      val observer = TestProbe("beta-observer")
      runOn(fourth, fifth) {
        Cluster(system).subscribe(observer.ref, InitialStateAsEvents, classOf[MemberUp])
      }
      val memberUpFromFifth = TestProbe("fromFifth")
      runOn(fourth) {
        system.actorOf(TestActors.forwardActorProps(memberUpFromFifth.ref), "fwFromFifth")
      }

      // all alpha nodes
      awaitClusterUp(first, second, third)

      runOn(fourth) {
        Cluster(system).join(first)
        within(20.seconds) {
          awaitAssert {
            Cluster(system).state.members
              .exists(m => m.address == address(fourth) && m.status == MemberStatus.Up) should ===(true)
          }
        }
      }

      // at the same time join fifth, which is the difference compared to MultiDcJoinSpec
      runOn(fifth) {
        Cluster(system).join(second)
        within(20.seconds) {
          awaitAssert {
            Cluster(system).state.members
              .exists(m => m.address == address(fifth) && m.status == MemberStatus.Up) should ===(true)
          }
        }
      }
      enterBarrier("beta-joined")

      runOn(fifth) {
        val events = observer
          .receiveN(5)
          .map(_.asInstanceOf[MemberUp])
          .filter(_.member.dataCenter == "beta")
          .sortBy(_.member.upNumber)

        events.head.member.upNumber should ===(1)
        events(1).member.upNumber should ===(2)

        // only sending Address since it's serializable
        events.foreach(evt => system.actorSelection(node(fourth) / "user" / "fwFromFifth") ! evt.member.address)
      }
      enterBarrier("fw-from-fifth")

      runOn(fourth) {
        val events4 = observer
          .receiveN(5)
          .map(_.asInstanceOf[MemberUp])
          .filter(_.member.dataCenter == "beta")
          .sortBy(_.member.upNumber)

        val upFromFifth = memberUpFromFifth.receiveN(2).map(_.asInstanceOf[Address])

        events4.head.member.address should ===(upFromFifth.head)

        events4.head.member.upNumber should ===(1)
        events4(1).member.upNumber should ===(2)

        observer.expectNoMessage(2.seconds)
      }

      enterBarrier("done")
    }

  }

}
