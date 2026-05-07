/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.actor.Address
import akka.testkit._

object ShutdownAfterJoinSeedNodesSpec {

  val config = """
       akka.actor.provider = "cluster"
       akka.coordinated-shutdown.terminate-actor-system = on
       akka.remote.artery.canonical.port = 0
       akka.cluster {
         seed-node-timeout = 2s
         retry-unsuccessful-join-after = 2s
         shutdown-after-unsuccessful-join-seed-nodes = 5s
         jmx.multi-mbeans-in-same-jvm = on
       }
       """
}

class ShutdownAfterJoinSeedNodesSpec extends AkkaSpec(ShutdownAfterJoinSeedNodesSpec.config) {

  val seed1 = ActorSystem(system.name, system.settings.config)
  val seed2 = ActorSystem(system.name, system.settings.config)
  val oridinary1 = ActorSystem(system.name, system.settings.config)

  override protected def afterTermination(): Unit = {
    shutdown(seed1)
    shutdown(seed2)
    shutdown(oridinary1)
  }

  "Joining seed nodes" must {
    "be aborted after shutdown-after-unsuccessful-join-seed-nodes" taggedAs LongRunningTest in {

      val seedNodes: immutable.IndexedSeq[Address] = Vector(seed1, seed2).map(s => Cluster(s).selfAddress)
      shutdown(seed1) // crash so that others will not be able to join

      Cluster(seed2).joinSeedNodes(seedNodes)
      Cluster(oridinary1).joinSeedNodes(seedNodes)

      Await.result(seed2.whenTerminated, Cluster(seed2).settings.ShutdownAfterUnsuccessfulJoinSeedNodes + 10.second)
      Await.result(
        oridinary1.whenTerminated,
        Cluster(seed2).settings.ShutdownAfterUnsuccessfulJoinSeedNodes + 10.second)
    }

  }
}
