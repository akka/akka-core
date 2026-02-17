/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.concurrent.Eventually.eventually

import akka.actor.{ Actor, ActorLogging, Address, Props }
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.sharding.ClusterShardingInstrumentationSpec.GiveMeYourHome.{ Get, Home }
import akka.cluster.sharding.internal.ClusterShardingInstrumentationProvider
import akka.remote.testkit.Direction
import akka.testkit.TestProbe
import akka.serialization.jackson.CborSerializable
import akka.testkit.ImplicitSender

object ClusterShardingInstrumentationSpecConfig
    extends MultiNodeClusterShardingConfig(
      //loglevel = "DEBUG",
      additionalConfig = """
      akka.cluster.sharding {
        rebalance-interval = 120 s
        telemetry.instrumentations += akka.cluster.sharding.ClusterShardingInstrumentationSpecTelemetry
        buffer-size = 120
      }
     """) {

  val first = role("first")
  val second = role("second")
  testTransport(on = true)

}

class ClusterShardingInstrumentationSpecMultiJvmNode1 extends ClusterShardingInstrumentationSpec

class ClusterShardingInstrumentationSpecMultiJvmNode2 extends ClusterShardingInstrumentationSpec

object ClusterShardingInstrumentationSpec {

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

abstract class ClusterShardingInstrumentationSpec
    extends MultiNodeClusterShardingSpec(ClusterShardingInstrumentationSpecConfig)
    with ImplicitSender
    with ScalaFutures {

  import ClusterShardingInstrumentationSpec._
  import ClusterShardingInstrumentationSpec.GiveMeYourHome._
  import ClusterShardingInstrumentationSpecConfig._

  private val shardRegionBufferSizeCounter = ClusterShardingInstrumentationProvider(system).instrumentation
    .asInstanceOf[ClusterShardingInstrumentationSpecTelemetry]
    .shardRegionBufferSizeCounter

  private val dropMessageCounter = ClusterShardingInstrumentationProvider(system).instrumentation
    .asInstanceOf[ClusterShardingInstrumentationSpecTelemetry]
    .dropMessageCounter

  val shardHomeRequests =
    ClusterShardingInstrumentationProvider(system).instrumentation
      .asInstanceOf[ClusterShardingInstrumentationSpecTelemetry]
      .shardHomeRequests

  val shardHomeResponses =
    ClusterShardingInstrumentationProvider(system).instrumentation
      .asInstanceOf[ClusterShardingInstrumentationSpecTelemetry]
      .shardHomeResponses

  override implicit val patienceConfig: PatienceConfig = {
    import akka.testkit.TestDuration
    PatienceConfig(testKitSettings.DefaultTimeout.duration.dilated, Span(1000, org.scalatest.time.Millis))
  }

  val typeName = "GiveMeYourHome"

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

        shardRegion ! Get("a")
        val address1 = expectMsgType[GiveMeYourHome.Home].address
        shardRegion ! Get("b")
        val address2 = expectMsgType[GiveMeYourHome.Home].address
        shardRegion ! Get("c")
        val address3 = expectMsgType[GiveMeYourHome.Home].address

        log.info(s"<<<<< ${List(address1, address2, address3)}")
      }
      enterBarrier("local-message-sent")
    }

    "cut traffic to coordinator" in {
      runOn(first) {
        testConductor.blackhole(first, second, Direction.Both).await
      }
      enterBarrier("blackhole-created")
    }

    "start shards to trigger buffering" in {
      runOn(second) {
        val probe = TestProbe()
        (1 to 100).foreach { n =>
          shardRegion.tell(Get(s"id-$n"), probe.ref)
        }
        eventually {
          shardRegionBufferSizeCounter.get() shouldBe 100
        }
      }
      enterBarrier("messages-buffered")
    }

    "let the buffer overflow" in {
      runOn(second) {
        val probe = TestProbe()
        (1 to 30).foreach { n =>
          shardRegion.tell(Get(s"id-$n"), probe.ref)
        }
        eventually {
          // we have 100 in the buffer, and our cap is 120 (per config in this test)
          // 10 are dropped. Should we have a metric on this? Or custom events?
          shardRegionBufferSizeCounter.get() shouldBe 120
          dropMessageCounter.get() shouldBe 10
        }
      }
      enterBarrier("buffer-overflow")
    }

    "resume traffic to coordinator" in {
      runOn(first) {
        testConductor.passThrough(first, second, Direction.Both).await
      }
      enterBarrier("blackhole-removed")
    }

    "record latency of requesting ShardHome" in {
      runOn(second) {
        shardHomeRequests.get() shouldBe shardHomeResponses.get()
      }
      enterBarrier("measure-shard-home-latency")
    }

    "clear buffered messages" in {
      runOn(second) {
        eventually(timeout(Span(5, Seconds))) {
          shardRegionBufferSizeCounter.get() shouldBe 0
        }
      }
      enterBarrier("messages-buffered-cleared")
    }

  }
}
