/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.Routers
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.cluster.typed.MultiNodeTypedClusterSpec
import akka.pattern.StatusReply
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.serialization.jackson.CborSerializable

object ShardedDaemonProcessRescaleSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
  val sixth = role("sixth")
  val seventh = role("seventh")

  val SnitchServiceKey = ServiceKey[AnyRef]("snitch")

  case class ProcessActorEvent(id: Int, event: Any) extends CborSerializable

  object ProcessActor {
    trait Command
    case object Stop extends Command

    def apply(id: Int): Behavior[Command] = Behaviors.setup { ctx =>
      ctx.log.info("Started [{}]", id)
      val snitchRouter = ctx.spawn(Routers.group(SnitchServiceKey), "router")
      snitchRouter ! ProcessActorEvent(id, "Started")

      Behaviors.receiveMessagePartial {
        case Stop =>
          ctx.log.info("Stopped [{}]", id)
          snitchRouter ! ProcessActorEvent(id, "Stopped")
          Behaviors.stopped
      }
    }
  }

  commonConfig(
    ConfigFactory.parseString("""
        akka.loglevel = DEBUG
        akka.cluster.sharded-daemon-process {
          sharding {
            # First is likely to be ignored as shard coordinator not ready
            retry-interval = 0.2s
          }
          # quick ping to make test swift, stress the start/stop guarantees during rescaling
          keep-alive-interval = 20 ms
          # disable throttle to stress the start/stop guarantees during rescaling
          keep-alive-throttle-interval = 0 s
        }
        akka.cluster.sharding {
          distributed-data.majority-min-cap = 4
          coordinator-state.write-majority-plus = 0
          coordinator-state.read-majority-plus = 0
        }
      """).withFallback(MultiNodeClusterSpec.clusterConfig))

}

class ShardedDaemonProcessRescaleMultiJvmNode1 extends ShardedDaemonProcessRescaleSpec
class ShardedDaemonProcessRescaleMultiJvmNode2 extends ShardedDaemonProcessRescaleSpec
class ShardedDaemonProcessRescaleMultiJvmNode3 extends ShardedDaemonProcessRescaleSpec
class ShardedDaemonProcessRescaleMultiJvmNode4 extends ShardedDaemonProcessRescaleSpec
class ShardedDaemonProcessRescaleMultiJvmNode5 extends ShardedDaemonProcessRescaleSpec
class ShardedDaemonProcessRescaleMultiJvmNode6 extends ShardedDaemonProcessRescaleSpec
class ShardedDaemonProcessRescaleMultiJvmNode7 extends ShardedDaemonProcessRescaleSpec

abstract class ShardedDaemonProcessRescaleSpec
    extends MultiNodeSpec(ShardedDaemonProcessRescaleSpec)
    with MultiNodeTypedClusterSpec
    with ScalaFutures {

  import ShardedDaemonProcessRescaleSpec._

  val topicProbe: TestProbe[AnyRef] = TestProbe[AnyRef]()
  private var sdp: ActorRef[ShardedDaemonProcessCommand] = _

  private def assertNumberOfProcesses(n: Int, revision: Int): Unit = {
    val probe = TestProbe[NumberOfProcesses]()
    sdp ! GetNumberOfProcesses(probe.ref)
    val reply = probe.receiveMessage()
    reply.numberOfProcesses should ===(n)
    reply.revision should ===(revision)
    reply.rescaleInProgress === (false)
  }

  "Cluster sharding in multi dc cluster" must {
    "form cluster" in {
      formCluster(first, second, third, fourth, fifth, sixth, seventh)
      runOn(first) {
        typedSystem.receptionist ! Receptionist.Register(SnitchServiceKey, topicProbe.ref, topicProbe.ref)
        topicProbe.expectMessageType[Receptionist.Registered]
      }
      enterBarrier("snitch-registered")

      topicProbe.awaitAssert({
        typedSystem.receptionist ! Receptionist.Find(SnitchServiceKey, topicProbe.ref)
        topicProbe.expectMessageType[Receptionist.Listing].serviceInstances(SnitchServiceKey).size should ===(1)
      }, 5.seconds)
      enterBarrier("snitch-seen")
    }

    "init actor set" in {
      sdp = ShardedDaemonProcess(typedSystem).initWithContext(
        "the-fearless",
        4,
        ctx => ProcessActor(ctx.processNumber),
        ShardedDaemonProcessSettings(system.toTyped),
        ProcessActor.Stop)
      enterBarrier("sharded-daemon-process-initialized")
      runOn(first) {
        val startedIds = (0 to 3).map { _ =>
          val event = topicProbe.expectMessageType[ProcessActorEvent](5.seconds)
          event.event should ===("Started")
          event.id
        }.toSet
        startedIds.size should ===(4)
        topicProbe.expectNoMessage()
      }
      enterBarrier("sharded-daemon-process-started-acked")
      runOn(third) {
        assertNumberOfProcesses(n = 4, revision = 0)
      }
      enterBarrier("sharded-daemon-process-started")
    }

    "rescale to 8 workers" in {
      runOn(first) {
        val probe = TestProbe[AnyRef]()
        sdp ! ChangeNumberOfProcesses(8, probe.ref)
        probe.expectMessage(30.seconds, StatusReply.Ack)
// FIXME snitch router is dropping messages
//        val events = topicProbe.receiveMessages(4 + 8, 10.seconds).map(_.asInstanceOf[ProcessActorEvent])
//        events.collect { case evt if evt.event == "Stopped" => evt.id }.toSet.size should ===(4)
//        events.collect { case evt if evt.event == "Started" => evt.id }.toSet.size should ===(8)
//        topicProbe.expectNoMessage()
      }

      enterBarrier("sharded-daemon-process-rescaled-to-8-acked")
      runOn(third) {
        assertNumberOfProcesses(n = 8, revision = 1)
      }
      enterBarrier("sharded-daemon-process-rescaled-to-8")
    }

    "rescale to 2 workers" in {
      runOn(second) {
        val probe = TestProbe[AnyRef]()
        sdp ! ChangeNumberOfProcesses(2, probe.ref)
        probe.expectMessage(30.seconds, StatusReply.Ack)
      }
      enterBarrier("sharded-daemon-process-rescaled-to-2-acked")
      runOn(first) {
// FIXME snitch router is dropping messages
//        val events = topicProbe.receiveMessages(8 + 2, 10.seconds).map(_.asInstanceOf[ProcessActorEvent])
//        events.collect { case evt if evt.event == "Stopped" => evt.id }.toSet.size should ===(8)
//        events.collect { case evt if evt.event == "Started" => evt.id }.toSet.size should ===(2)
//        topicProbe.expectNoMessage()
      }
      runOn(third) {
        assertNumberOfProcesses(n = 2, revision = 2)
      }
      enterBarrier("sharded-daemon-process-rescaled-to-2")
    }

    "query the state" in {
      runOn(third) {
        assertNumberOfProcesses(n = 2, revision = 2)
      }
      enterBarrier("sharded-daemon-process-query")
    }

  }
}
