/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package scala.docs.cluster

import akka.cluster.Cluster
import akka.testkit.AkkaSpec
import docs.CompileOnlySpec

import scala.annotation.nowarn

object ClusterDocSpec {

  val config =
    """
    akka.actor.provider = "cluster"
    """
}

@nowarn("msg=deprecated")
@nowarn("msg=never used") // sample snippets
class ClusterDocSpec extends AkkaSpec(ClusterDocSpec.config) with CompileOnlySpec {

  "demonstrate leave" in compileOnlySpec {
    //#leave
    val cluster = Cluster(system)
    cluster.leave(cluster.selfAddress)
    //#leave
  }

  "demonstrate data center" in compileOnlySpec {
    {
      //#dcAccess
      val cluster = Cluster(system)
      // this node's data center
      val dc = cluster.selfDataCenter
      // all known data centers
      val allDc = cluster.state.allDataCenters
      // a specific member's data center
      val aMember = cluster.state.members.head
      val aDc = aMember.dataCenter
      //#dcAccess
    }
  }

  "demonstrate programmatic joining to seed nodes" in compileOnlySpec {
    //#join-seed-nodes
    import akka.actor.Address
    import akka.cluster.Cluster

    val cluster = Cluster(system)
    val list: List[Address] = ??? //your method to dynamically get seed nodes
    cluster.joinSeedNodes(list)
    //#join-seed-nodes
  }

}
