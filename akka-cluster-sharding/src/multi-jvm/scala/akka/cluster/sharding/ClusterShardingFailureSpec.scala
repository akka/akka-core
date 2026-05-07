/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import akka.actor._
import akka.cluster.sharding.ShardRegion.Passivate
import akka.cluster.sharding.ShardRegion.StartEntity
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.serialization.jackson.CborSerializable
import akka.testkit._

object ClusterShardingFailureSpec {
  case class Get(id: String) extends CborSerializable
  case class Add(id: String, i: Int) extends CborSerializable
  case class Value(id: String, n: Int) extends CborSerializable

  class Entity extends Actor with ActorLogging {
    log.debug("Starting")
    var n = 0

    def receive = {
      case Get(id) =>
        log.debug("Got get request from {}", sender())
        sender() ! Value(id, n)
      case Add(_, i) =>
        n += i
        log.debug("Got add request from {}", sender())
    }

    override def postStop(): Unit = log.debug("Stopping")
  }

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case m @ Get(id)    => (id, m)
    case m @ Add(id, _) => (id, m)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case Get(id)         => id.charAt(0).toString
    case Add(id, _)      => id.charAt(0).toString
    case StartEntity(id) => id
    case _               => throw new IllegalArgumentException()
  }
}

abstract class ClusterShardingFailureSpecConfig(override val mode: String)
    extends MultiNodeClusterShardingConfig(
      mode,
      additionalConfig = s"""
        akka.loglevel=DEBUG
        akka.cluster.roles = ["backend"]
        akka.cluster.sharding {
          coordinator-failure-backoff = 3s
          shard-failure-backoff = 3s
        }
        # don't leak ddata state across runs
        akka.cluster.sharding.distributed-data.durable.keys = []
        akka.persistence.journal.leveldb-shared.store.native = off
        # using Java serialization for these messages because test is sending them
        # to other nodes, which isn't normal usage.
        akka.actor.serialization-bindings {
          "${classOf[ShardRegion.Passivate].getName}" = java-test
        }
        """,
      rememberEntities = true) {

  val controller = role("controller")
  val first = role("first")
  val second = role("second")

  testTransport(on = true)
}

object PersistentClusterShardingFailureSpecConfig
    extends ClusterShardingFailureSpecConfig(ClusterShardingSettings.StateStoreModePersistence)
object DDataClusterShardingFailureSpecConfig
    extends ClusterShardingFailureSpecConfig(ClusterShardingSettings.StateStoreModeDData)

class PersistentClusterShardingFailureSpec
    extends ClusterShardingFailureSpec(PersistentClusterShardingFailureSpecConfig)
class DDataClusterShardingFailureSpec extends ClusterShardingFailureSpec(DDataClusterShardingFailureSpecConfig)

class PersistentClusterShardingFailureMultiJvmNode1 extends PersistentClusterShardingFailureSpec
class PersistentClusterShardingFailureMultiJvmNode2 extends PersistentClusterShardingFailureSpec
class PersistentClusterShardingFailureMultiJvmNode3 extends PersistentClusterShardingFailureSpec

class DDataClusterShardingFailureMultiJvmNode1 extends DDataClusterShardingFailureSpec
class DDataClusterShardingFailureMultiJvmNode2 extends DDataClusterShardingFailureSpec
class DDataClusterShardingFailureMultiJvmNode3 extends DDataClusterShardingFailureSpec

abstract class ClusterShardingFailureSpec(multiNodeConfig: ClusterShardingFailureSpecConfig)
    extends MultiNodeClusterShardingSpec(multiNodeConfig)
    with ImplicitSender {
  import ClusterShardingFailureSpec._
  import multiNodeConfig._

  def join(from: RoleName, to: RoleName): Unit = {
    join(
      from,
      to,
      startSharding(
        system,
        typeName = "Entity",
        entityProps = Props[Entity](),
        extractEntityId = extractEntityId,
        extractShardId = extractShardId))
  }

  lazy val region = ClusterSharding(system).shardRegion("Entity")

  s"Cluster sharding ($mode) with flaky journal/network" must {

    "join cluster" in within(20.seconds) {
      startPersistenceIfNeeded(startOn = controller, setStoreOn = Seq(first, second))

      join(first, first)
      join(second, first)

      runOn(first) {
        region ! Add("10", 1)
        region ! Add("20", 2)
        region ! Add("21", 3)
        region ! Get("10")
        expectMsg(Value("10", 1))
        region ! Get("20")
        expectMsg(Value("20", 2))
        region ! Get("21")
        expectMsg(Value("21", 3))
      }

      enterBarrier("after-2")
    }

    "recover after journal/network failure" in within(20.seconds) {
      runOn(controller) {
        if (persistenceIsNeeded) {
          testConductor.blackhole(controller, first, Direction.Both).await
          testConductor.blackhole(controller, second, Direction.Both).await
        } else {
          testConductor.blackhole(first, second, Direction.Both).await
        }
      }
      enterBarrier("journal-blackholed")

      runOn(first) {
        // try with a new shard, will not reply until journal/network is available again
        region ! Add("40", 4)
        val probe = TestProbe()
        region.tell(Get("40"), probe.ref)
        probe.expectNoMessage(1.second)
      }

      enterBarrier("first-delayed")

      runOn(controller) {
        if (persistenceIsNeeded) {
          testConductor.passThrough(controller, first, Direction.Both).await
          testConductor.passThrough(controller, second, Direction.Both).await
        } else {
          testConductor.passThrough(first, second, Direction.Both).await
        }
      }
      enterBarrier("journal-ok")

      runOn(first) {
        region ! Get("21")
        expectMsg(Value("21", 3))
        val entity21 = lastSender
        val shard2 = system.actorSelection(entity21.path.parent)

        //Test the ShardCoordinator allocating shards after a journal/network failure
        region ! Add("30", 3)

        //Test the Shard starting entities and persisting after a journal/network failure
        region ! Add("11", 1)

        //Test the Shard passivate works after a journal failure
        shard2.tell(Passivate(PoisonPill), entity21)

        awaitAssert {
          // Note that the order between this Get message to 21 and the above Passivate to 21 is undefined.
          // If this Get arrives first the reply will be Value("21", 3) and then it is retried by the
          // awaitAssert.
          // Also note that there is no timeout parameter on below expectMsg because messages should not
          // be lost here. They should be buffered and delivered also after Passivate completed.
          region ! Get("21")
          // counter reset to 0 when started again
          expectMsg(Value("21", 0))
        }

        region ! Add("21", 1)

        region ! Get("21")
        expectMsg(Value("21", 1))

        region ! Get("30")
        expectMsg(Value("30", 3))

        region ! Get("11")
        expectMsg(Value("11", 1))

        region ! Get("40")
        expectMsg(Value("40", 4))
      }

      enterBarrier("verified-first")

      runOn(second) {
        region ! Add("10", 1)
        region ! Add("20", 2)
        region ! Add("30", 3)
        region ! Add("11", 4)
        region ! Get("10")
        expectMsg(Value("10", 2))
        region ! Get("11")
        expectMsg(Value("11", 5))
        region ! Get("20")
        expectMsg(Value("20", 4))
        region ! Get("30")
        expectMsg(Value("30", 6))
      }

      enterBarrier("after-3")

    }

  }
}
