/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.scaladsl

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.ChangeNumberOfProcesses
import akka.cluster.sharding.typed.ShardedDaemonProcessCommand
import akka.cluster.sharding.typed.ShardedDaemonProcessContext
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.pattern.StatusReply

object ShardedDaemonProcessRescaleSpec {
  // single node cluster config
  def config = ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.actor.testkit.typed.single-expect-default = 10s

      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1

      akka.cluster.jmx.multi-mbeans-in-same-jvm = on

      # ping often/start fast for test
      akka.cluster.sharded-daemon-process.keep-alive-interval = 1s
      akka.cluster.sharded-daemon-process.keep-alive-throttle-interval = 20ms

      akka.coordinated-shutdown.terminate-actor-system = off
      akka.coordinated-shutdown.run-by-actor-system-terminate = off
      """)

  object ScalingActor {
    sealed trait Command
    case object Stop extends Command

    case class Started(id: Int, totalCount: Int, selfRef: ActorRef[Command])

    case class Stopping(selfRef: ActorRef[Command])

    def apply(id: Int, totalCount: Int, probe: ActorRef[Any]): Behavior[Command] = Behaviors.setup { ctx =>
      probe ! Started(id, totalCount, ctx.self)

      Behaviors.receiveMessage {
        case Stop =>
          probe ! Stopping(ctx.self)
          Behaviors.stopped
      }
    }

  }

}

class ShardedDaemonProcessRescaleSpec
    extends ScalaTestWithActorTestKit(ShardedDaemonProcessRescaleSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import ShardedDaemonProcessRescaleSpec._

  private var sdp: ActorRef[ShardedDaemonProcessCommand] = _
  private val workerLifecycleProbe: TestProbe[Any] = createTestProbe[Any]()

  "The ShardedDaemonProcess" must {

    "have a single node cluster running first" in {
      val probe = createTestProbe()
      Cluster(system).manager ! Join(Cluster(system).selfMember.address)
      probe.awaitAssert({
        Cluster(system).selfMember.status == MemberStatus.Up
      }, 3.seconds)
    }

    "start 4 workers" in {
      sdp = ShardedDaemonProcess(system).initWithContext[ScalingActor.Command](
        "a",
        4,
        (ctx: ShardedDaemonProcessContext) =>
          ScalingActor(ctx.processNumber, ctx.totalProcesses, workerLifecycleProbe.ref),
        ShardedDaemonProcessSettings(system),
        Some(ScalingActor.Stop),
        None)

      val started = (1 to 4).map(_ => workerLifecycleProbe.expectMessageType[ScalingActor.Started]).toSet
      started.size should ===(4)
      started.head.totalCount should ===(4)
      started.map(_.id) should ===(Set(0, 1, 2, 3))
    }

    "rescale to 16 workers" in {
      val rescaleProbe = createTestProbe[StatusReply[Done]]()
      sdp ! ChangeNumberOfProcesses(16, rescaleProbe.ref)

      val stoppedRefs = (1 to 4).map(_ => workerLifecycleProbe.expectMessageType[ScalingActor.Stopping].selfRef).toSet
      stoppedRefs.size should ===(4) // 4 different actors stopped

      val secondRoundStarted = (1 to 16).map(_ => workerLifecycleProbe.expectMessageType[ScalingActor.Started]).toSet
      secondRoundStarted.map(_.selfRef).size should ===(16) // 16 different actors started

      rescaleProbe.expectMessage(StatusReply.Ack)
    }

    "rescale to 2 workers" in {
      val rescaleProbe = createTestProbe[StatusReply[Done]]()
      sdp ! ChangeNumberOfProcesses(2, rescaleProbe.ref)

      val stoppedRefs = (1 to 16).map(_ => workerLifecycleProbe.expectMessageType[ScalingActor.Stopping].selfRef).toSet
      stoppedRefs.size should ===(16) // 4 different actors stopped

      val thirdRoundStarted = (1 to 2).map(_ => workerLifecycleProbe.expectMessageType[ScalingActor.Started]).toSet
      thirdRoundStarted.map(_.selfRef).size should ===(2)

      rescaleProbe.expectMessage(StatusReply.Ack)
    }
  }
}
