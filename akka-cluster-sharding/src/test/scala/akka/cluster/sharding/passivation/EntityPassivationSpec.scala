/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing
import akka.util.Clock

object EntityPassivationSpec {

  val config: Config = ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    akka.scheduled-clock-interval = 100 ms
    """)

  val disabledConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = none
      }
    }
    """).withFallback(config)

  object Entity {
    case object Stop
    case object IgnoreStop
    case object ManuallyPassivate
    case class Envelope(shard: Int, id: Int, message: Any)
    case class Received(id: String, message: Any, nanoTime: Long)

    def props(probes: Map[String, ActorRef]) = Props(new Entity(probes))
  }

  class Entity(probes: Map[String, ActorRef]) extends Actor {
    private val clock = Clock(context.system)
    def id = context.self.path.name

    def received(message: Any) = probes(id) ! Entity.Received(id, message, clock.currentTime())

    def receive = {
      case Entity.Stop =>
        received(Entity.Stop)
        context.stop(self)
      case Entity.IgnoreStop =>
        received(Entity.IgnoreStop)
        unhandled(Entity.IgnoreStop)
      case Entity.ManuallyPassivate =>
        received(Entity.ManuallyPassivate)
        context.parent ! ShardRegion.Passivate(Entity.Stop)
      case msg => received(msg)
    }
  }

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case Entity.Envelope(_, id, message) => (id.toString, message)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case Entity.Envelope(shard, _, _) => shard.toString
    case _                            => throw new IllegalArgumentException
  }
}

abstract class AbstractEntityPassivationSpec(config: Config, expectedEntities: Int)
    extends AkkaSpec(config)
    with Eventually
    with WithLogCapturing {

  import EntityPassivationSpec._

  val settings: ClusterShardingSettings = ClusterShardingSettings(system)
  val configuredIdleTimeout: FiniteDuration =
    settings.passivationStrategySettings.idleEntitySettings.fold(Duration.Zero)(_.timeout)
  val configuredActiveEntityLimit: Int = settings.passivationStrategySettings.activeEntityLimit.getOrElse(0)

  lazy val clock: Clock = Clock(system)

  val probes: Map[Int, TestProbe] = (1 to expectedEntities).map(id => id -> TestProbe()).toMap
  val probeRefs: Map[String, ActorRef] = probes.map { case (id, probe) => id.toString -> probe.ref }
  val stateProbe: TestProbe = TestProbe()

  def expectReceived(id: Int, message: Any, within: FiniteDuration = patience.timeout): Entity.Received = {
    val received = probes(id).expectMsgType[Entity.Received](within)
    received.message shouldBe message
    received
  }

  def expectNoMessage(id: Int, within: FiniteDuration): Unit =
    probes(id).expectNoMessage(within)

  def getState(region: ActorRef): ShardRegion.CurrentShardRegionState = {
    region.tell(ShardRegion.GetShardRegionState, stateProbe.ref)
    stateProbe.expectMsgType[ShardRegion.CurrentShardRegionState]
  }

  def expectState(region: ActorRef)(expectedShards: (Int, Iterable[Int])*): Unit =
    eventually {
      getState(region).shards should contain theSameElementsAs expectedShards.map {
        case (shardId, entityIds) => ShardRegion.ShardState(shardId.toString, entityIds.map(_.toString).toSet)
      }
    }

  def start(stopMessage: Any = Entity.Stop): ActorRef = {
    // single node cluster
    Cluster(system).join(Cluster(system).selfAddress)
    ClusterSharding(system).start(
      "myType",
      EntityPassivationSpec.Entity.props(probeRefs),
      settings,
      extractEntityId,
      extractShardId,
      ClusterSharding(system).defaultShardAllocationStrategy(settings),
      stopMessage)
  }
}

class DisabledEntityPassivationSpec
    extends AbstractEntityPassivationSpec(EntityPassivationSpec.disabledConfig, expectedEntities = 1) {

  import EntityPassivationSpec.Entity.Envelope

  "Passivation of idle entities" must {
    "not passivate when passivation is disabled" in {
      settings.passivationStrategy shouldBe ClusterShardingSettings.NoPassivationStrategy
      val region = start()
      region ! Envelope(shard = 1, id = 1, message = "A")
      expectReceived(id = 1, message = "A")
      expectNoMessage(id = 1, 1.second)
    }
  }
}
