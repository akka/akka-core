/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Span

import akka.Done
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.cluster.typed.ClusterSingleton
import akka.cluster.typed.MultiNodeTypedClusterSpec
import akka.cluster.typed.SingletonActor
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

  case class ProcessActorEvent(id: Int, event: Any) extends CborSerializable

  object ProcessActor {
    trait Command
    case object Stop extends Command

    def apply(id: Int, collector: ActorRef[Collector.Command]): Behavior[Command] = Behaviors.setup { ctx =>
      ctx.log.info("Started [{}]", id)
      collector ! Collector.Started(id)

      Behaviors.receiveMessagePartial {
        case Stop =>
          ctx.log.info("Stopped [{}]", id)
          collector ! Collector.Stopped(id)
          Behaviors.stopped
      }
    }
  }

  object Collector {
    sealed trait Command extends CborSerializable
    final case class Started(id: Int) extends Command
    final case class Stopped(id: Int) extends Command
    final case class Get(replyTo: ActorRef[Counts]) extends Command
    final case class Counts(startedCount: Int, stoppedCount: Int) extends CborSerializable
    final case class Reset(replyTo: ActorRef[Done]) extends Command

    def init(system: ActorSystem[_]): ActorRef[Command] = {
      ClusterSingleton(system).init(SingletonActor(Collector(), "collector"))
    }

    def apply(): Behavior[Command] = {
      behavior(Counts(startedCount = 0, stoppedCount = 0))
    }

    private def behavior(counts: Counts): Behavior[Command] = {
      Behaviors.receiveMessage {
        case Started(_) =>
          behavior(counts.copy(startedCount = counts.startedCount + 1))
        case Stopped(_) =>
          behavior(counts.copy(stoppedCount = counts.stoppedCount + 1))
        case Get(replyTo) =>
          replyTo ! counts
          Behaviors.same
        case Reset(replyTo) =>
          replyTo ! Done
          behavior(Counts(0, 0))
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
    with ScalaFutures
    with Eventually {

  import ShardedDaemonProcessRescaleSpec._

  implicit val patience: PatienceConfig = {
    import akka.testkit.TestDuration
    PatienceConfig(testKitSettings.DefaultTimeout.duration.dilated * 2, Span(500, org.scalatest.time.Millis))
  }

  private var sdp: ActorRef[ShardedDaemonProcessCommand] = _
  private var collector: ActorRef[Collector.Command] = _
  private val resetProbe = TestProbe[Done]()

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

      collector = Collector.init(system.toTyped)
      enterBarrier("collector-started")
    }

    "init actor set" in {
      sdp = ShardedDaemonProcess(typedSystem).initWithContext(
        "the-fearless",
        4,
        ctx => ProcessActor(ctx.processNumber, collector),
        ShardedDaemonProcessSettings(system.toTyped),
        ProcessActor.Stop)
      enterBarrier("sharded-daemon-process-initialized")
      eventually {
        val countsReplyProbe = TestProbe[Collector.Counts]()
        collector ! Collector.Get(countsReplyProbe.ref)
        countsReplyProbe.expectMessage(500.millis, Collector.Counts(startedCount = 4, stoppedCount = 0))
      }
      enterBarrier("sharded-daemon-process-started-acked")
      runOn(third) {
        assertNumberOfProcesses(n = 4, revision = 0)
      }
      enterBarrier("sharded-daemon-process-started")
    }

    "rescale to 8 workers" in {
      runOn(first) {
        collector ! Collector.Reset(resetProbe.ref)
        resetProbe.expectMessage(Done)

        val probe = TestProbe[AnyRef]()
        sdp ! ChangeNumberOfProcesses(8, probe.ref)
        probe.expectMessage(30.seconds, StatusReply.Ack)
      }
      enterBarrier("sharded-daemon-process-rescaled-to-8")
      eventually {
        val countsReplyProbe = TestProbe[Collector.Counts]()
        collector ! Collector.Get(countsReplyProbe.ref)
        countsReplyProbe.expectMessage(500.millis, Collector.Counts(startedCount = 8, stoppedCount = 4))
      }

      enterBarrier("sharded-daemon-process-rescaled-to-8-acked")
      runOn(third) {
        assertNumberOfProcesses(n = 8, revision = 1)
      }
      enterBarrier("sharded-daemon-process-rescaled-to-8")
    }

    "rescale to 2 workers" in {
      runOn(second) {
        collector ! Collector.Reset(resetProbe.ref)
        resetProbe.expectMessage(Done)

        val probe = TestProbe[AnyRef]()
        sdp ! ChangeNumberOfProcesses(2, probe.ref)
        probe.expectMessage(30.seconds, StatusReply.Ack)
      }
      enterBarrier("sharded-daemon-process-rescaled-to-2")
      eventually {
        val countsReplyProbe = TestProbe[Collector.Counts]()
        collector ! Collector.Get(countsReplyProbe.ref)
        countsReplyProbe.expectMessage(500.millis, Collector.Counts(startedCount = 2, stoppedCount = 8))
      }

      enterBarrier("sharded-daemon-process-rescaled-to-2-acked")
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
