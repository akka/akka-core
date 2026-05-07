/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.remote.testkit._
import akka.testkit.ImplicitSender

object InitialMembersOfNewDcSpec extends MultiNodeConfig {
  commonConfig(ConfigFactory.parseString("""
    akka.actor.provider = cluster
    akka.actor.warn-about-java-serializer-usage = off
    akka.cluster {
      jmx.enabled                         = off
      debug.verbose-gossip-logging = on
    }
    akka.cluster.multi-data-center {
       #cross-data-center-gossip-probability = 0.5
    }
    akka.loglevel = INFO
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.loggers = ["akka.testkit.TestEventListener"]
    """))

  val one = role("one")
  val two = role("two")
  val three = role("three")

  val four = role("four")
  val five = role("five")

  nodeConfig(one, two, three) {
    ConfigFactory.parseString("akka.cluster.multi-data-center.self-data-center = DC1")
  }

  nodeConfig(four, five) {
    ConfigFactory.parseString("akka.cluster.multi-data-center.self-data-center = DC2")
  }

}

class InitialMembersOfNewDcSpecMultiJvmNode1 extends InitialMembersOfNewDcSpec
class InitialMembersOfNewDcSpecMultiJvmNode2 extends InitialMembersOfNewDcSpec
class InitialMembersOfNewDcSpecMultiJvmNode3 extends InitialMembersOfNewDcSpec
class InitialMembersOfNewDcSpecMultiJvmNode4 extends InitialMembersOfNewDcSpec
class InitialMembersOfNewDcSpecMultiJvmNode5 extends InitialMembersOfNewDcSpec

abstract class InitialMembersOfNewDcSpec
    extends MultiNodeSpec(InitialMembersOfNewDcSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import InitialMembersOfNewDcSpec._

  def initialParticipants = roles.size
  val cluster = Cluster(system)

  "Joining a new DC" must {
    "join node one" in {
      runOn(one) {
        cluster.join(node(one).address)
      }
      enterBarrier("node one up")
    }

    "see all dc1 nodes join" in {
      runOn(two, three) {
        cluster.join(node(one).address)
      }
    }

    "see all dc1 nodes see each other as up" in {
      runOn(two, three) {
        within(20.seconds) {
          awaitAssert({
            cluster.state.members.filter(_.status == MemberStatus.Up) should have size 3
          })
        }
      }
      enterBarrier("dc1 fully up")
    }

    "join first member of new dc" in {
      enterBarrier("Node 4 about to join")
      val startTime = System.nanoTime()
      runOn(four) {
        log.info("Joining cluster")
        cluster.join(node(one).address)
      }

      // Check how long it takes for all other nodes to see every node as up
      runOn(one, two, three, four) {
        within(20.seconds) {
          awaitAssert({
            cluster.state.members.filter(_.status == MemberStatus.Up) should have size 4
          })
        }
        val totalTime = System.nanoTime() - startTime
        log.info("Can see new node (and all others as up): {}ms", totalTime.nanos.toMillis)
      }
      enterBarrier("node 4 joined dc and all nodes know it is up")
    }
  }
}
