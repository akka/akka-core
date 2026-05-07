/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor.Address
import akka.routing.ActorSelectionRoutee
import akka.routing.ConsistentHash
import akka.routing.ConsistentRoutee
import akka.testkit.AkkaSpec

class RemoteConsistentHashingRouterSpec
    extends AkkaSpec("""
    akka.remote.artery.canonical.port = 0                                                         
    akka.actor.provider = remote """) {

  "ConsistentHashingGroup" must {

    "use same hash ring independent of self address" in {
      // simulating running router on two different nodes (a1, a2) with target routees on 3 other nodes (s1, s2, s3)
      val a1 = Address("akka", "Sys", "client1", 2552)
      val a2 = Address("akka", "Sys", "client2", 2552)
      val s1 = ActorSelectionRoutee(system.actorSelection("akka://Sys@server1:2552/user/a/b"))
      val s2 = ActorSelectionRoutee(system.actorSelection("akka://Sys@server2:2552/user/a/b"))
      val s3 = ActorSelectionRoutee(system.actorSelection("akka://Sys@server3:2552/user/a/b"))
      val nodes1 = List(ConsistentRoutee(s1, a1), ConsistentRoutee(s2, a1), ConsistentRoutee(s3, a1))
      val nodes2 = List(ConsistentRoutee(s1, a2), ConsistentRoutee(s2, a2), ConsistentRoutee(s3, a2))
      val consistentHash1 = ConsistentHash(nodes1, 10)
      val consistentHash2 = ConsistentHash(nodes2, 10)
      val keys = List("A", "B", "C", "D", "E", "F", "G")
      val result1 = keys.collect { case k => consistentHash1.nodeFor(k).routee }
      val result2 = keys.collect { case k => consistentHash2.nodeFor(k).routee }
      result1 should ===(result2)
    }

  }

}
