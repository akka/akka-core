/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Span
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually.eventually

import akka.actor.ExtendedActorSystem
import akka.actor.{ Actor, ActorLogging, Address, Props }
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.sharding.ClusterShardInstrumentatioSpec.GiveMeYourHome.{ Get, Home }
import akka.cluster.sharding.internal.ClusterShardingInstrumentation
import akka.remote.testkit.Direction
import akka.testkit.TestProbe
import akka.serialization.jackson.CborSerializable
import akka.testkit.ImplicitSender

object ClusterShardInstrumentatioSpecConfig
    extends MultiNodeClusterShardingConfig(
      //loglevel = "DEBUG",
      additionalConfig = """
      akka.cluster.sharding {
        rebalance-interval = 120 s
        telemetry.instrumentations += akka.cluster.sharding.SpecClusterShardingTelemetry
      }
     """) {

  val first = role("first")
  val second = role("second")
  testTransport(on = true)

  val counter = new AtomicInteger()
}

class ClusterShardInstrumentatioSpecMultiJvmNode1 extends ClusterShardInstrumentatioSpec

class ClusterShardInstrumentatioSpecMultiJvmNode2 extends ClusterShardInstrumentatioSpec

class SpecClusterShardingTelemetry(
    @nowarn("msg=never used") scope: String,
    @nowarn("msg=never used") typeName: String,
    @nowarn("msg=never used") system: ExtendedActorSystem)
    extends ClusterShardingInstrumentation {

  override def shardBufferSize(size: Int): Unit = {

    ClusterShardInstrumentatioSpecConfig.counter.set(size)
  }

  override def increaseShardBufferSize(): Unit = {
    ClusterShardInstrumentatioSpecConfig.counter.incrementAndGet()
  }

  override def dependencies: Seq[String] = Nil
}

object ClusterShardInstrumentatioSpec {

  object GiveMeYourHome {
    case class Get(id: String) extends CborSerializable

    case class Home(address: Address) extends CborSerializable

    val extractEntityId: ShardRegion.ExtractEntityId = {
      case g @ Get(id) => (id, g)
    }

    // shard == id to make testing easier
    val extractShardId: ShardRegion.ExtractShardId = {
      case Get(id) => id
      case _       => throw new IllegalArgumentException()
    }
  }

  class GiveMeYourHome extends Actor with ActorLogging {

    val selfAddress = Cluster(context.system).selfAddress

    log.info("Started on {}", selfAddress)

    override def receive: Receive = {
      case Get(_) =>
        sender() ! Home(selfAddress)
    }
  }
}

abstract class ClusterShardInstrumentatioSpec
    extends MultiNodeClusterShardingSpec(ClusterShardInstrumentatioSpecConfig)
    with ImplicitSender
    with ScalaFutures {

  import ClusterShardInstrumentatioSpec._
  import ClusterShardInstrumentatioSpec.GiveMeYourHome._
  import ClusterShardInstrumentatioSpecConfig._

  override implicit val patienceConfig: PatienceConfig = {
    import akka.testkit.TestDuration
    PatienceConfig(testKitSettings.DefaultTimeout.duration.dilated, Span(1000, org.scalatest.time.Millis))
  }

  val typeName = "GiveMeYourHome"
  val initiallyOnForth = "on-fourth"

  def shardRegion =
    startSharding(
      system,
      typeName = typeName,
      entityProps = Props[GiveMeYourHome](),
      extractEntityId = extractEntityId,
      extractShardId = extractShardId)

  "External shard allocation" must {
    "join cluster" in within(20.seconds) {

      join(first, first, onJoinedRunOnFrom = shardRegion)
      join(second, first, onJoinedRunOnFrom = shardRegion, assertNodeUp = false)

      // all Up, everywhere before continuing
      runOn(first, second) {
        awaitAssert {
          cluster.state.members.size should ===(2)
          cluster.state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        }
      }

      enterBarrier("after-2")
    }

    "default to allocating a shard to the local shard region" in {
      runOn(first, second) {

        shardRegion ! Get("id1")
        val address1 = expectMsgType[GiveMeYourHome.Home].address
        shardRegion ! Get("id2")
        val address2 = expectMsgType[GiveMeYourHome.Home].address
        shardRegion ! Get("id3")
        val address3 = expectMsgType[GiveMeYourHome.Home].address

        log.info(s"<<<<< ${List(address1, address2, address3)}")
      }
      enterBarrier("local-message-sent")
    }

    "cut traffic to coordinator" in {
      runOn(first) {
        testConductor.blackhole(first, second, Direction.Both).await
      }
    }

    "start shards to trigger buffering" in {
      runOn(second) {
        val probe = TestProbe()
        (1 to 100).foreach { n =>
          shardRegion.tell(Get(s"id-$n"), probe.ref)
        }
        eventually {
          ClusterShardInstrumentatioSpecConfig.counter.get() shouldBe 100
        }
      }
    }
  }
}
