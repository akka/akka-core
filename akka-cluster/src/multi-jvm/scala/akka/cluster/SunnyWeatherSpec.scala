/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.atomic.AtomicReference

import scala.collection.immutable.SortedSet

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.Props
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object SunnyWeatherMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  // Note that this test uses default configuration,
  // not MultiNodeClusterSpec.clusterConfig
  commonConfig(ConfigFactory.parseString("""
    akka {
      actor.provider = cluster
      loggers = ["akka.testkit.TestEventListener"]
      loglevel = INFO
      cluster {
        failure-detector.monitored-by-nr-of-members = 3
        downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        split-brain-resolver.active-strategy = keep-majority
      }
    }
    """))

}

class SunnyWeatherMultiJvmNode1 extends SunnyWeatherSpec
class SunnyWeatherMultiJvmNode2 extends SunnyWeatherSpec
class SunnyWeatherMultiJvmNode3 extends SunnyWeatherSpec
class SunnyWeatherMultiJvmNode4 extends SunnyWeatherSpec
class SunnyWeatherMultiJvmNode5 extends SunnyWeatherSpec

abstract class SunnyWeatherSpec extends MultiNodeClusterSpec(SunnyWeatherMultiJvmSpec) {

  import ClusterEvent._
  import SunnyWeatherMultiJvmSpec._

  "A normal cluster" must {
    "be healthy" taggedAs LongRunningTest in {

      // start some
      awaitClusterUp(first, second, third)
      runOn(first, second, third) {
        log.debug("3 joined")
      }

      // add a few more
      awaitClusterUp(roles: _*)
      log.debug("5 joined")

      val unexpected = new AtomicReference[SortedSet[Member]](SortedSet.empty)
      cluster.subscribe(system.actorOf(Props(new Actor {
        def receive = {
          case event: MemberEvent =>
            // we don't expected any changes to the cluster
            unexpected.set(unexpected.get + event.member)
          case _: CurrentClusterState => // ignore
        }
      })), classOf[MemberEvent])

      for (n <- 1 to 30) {
        enterBarrier("period-" + n)
        unexpected.get should ===(SortedSet.empty)
        awaitMembersUp(roles.size)
        assertLeaderIn(roles)
        if (n % 5 == 0) log.debug("Passed period [{}]", n)
        Thread.sleep(1000)
      }

      enterBarrier("after")
    }
  }
}
