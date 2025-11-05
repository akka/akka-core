/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import akka.actor.Props
import akka.cluster.Cluster
import akka.testkit.AkkaSpec
import akka.testkit.TestActors.EchoActor
import akka.testkit.WithLogCapturing

object GetShardTypeNamesSpec {
  val config =
    """
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    """

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
    case _        => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int => (msg % 10).toString
    case _        => throw new IllegalArgumentException()
  }
}

class GetShardTypeNamesSpec extends AkkaSpec(GetShardTypeNamesSpec.config) with WithLogCapturing {
  import GetShardTypeNamesSpec._

  "GetShardTypeNames" must {
    "contain empty when join cluster without shards" in {
      ClusterSharding(system).shardTypeNames should ===(Set.empty[String])
    }

    "contain started shards when started 2 shards" in {
      Cluster(system).join(Cluster(system).selfAddress)
      val settings = ClusterShardingSettings(system)
      ClusterSharding(system).start("type1", Props[EchoActor](), settings, extractEntityId, extractShardId)
      ClusterSharding(system).start("type2", Props[EchoActor](), settings, extractEntityId, extractShardId)

      ClusterSharding(system).shardTypeNames should ===(Set("type1", "type2"))
    }
  }
}
