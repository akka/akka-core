/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.DeadLettersFilter
import akka.testkit.TestEvent.Mute
import akka.testkit.WithLogCapturing

object ConcurrentStartupShardingSpec {

  val config =
    """
    akka.actor.provider = "cluster"
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.remote.artery.canonical.port = 0
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.cluster.sharding.verbose-debug-logging = on
    akka.actor {
      default-dispatcher {
        executor = "fork-join-executor"
        fork-join-executor {
          parallelism-min = 5
          parallelism-max = 5
        }
      }
    }
    """

  object Starter {
    def props(n: Int, probe: ActorRef): Props =
      Props(new Starter(n, probe))
  }

  class Starter(n: Int, probe: ActorRef) extends Actor {

    override def preStart(): Unit = {
      val region =
        ClusterSharding(context.system).start(s"type-$n", Props.empty, ClusterShardingSettings(context.system), {
          case msg => (msg.toString, msg)
        }, _ => "1")
      probe ! region
    }

    def receive = {
      case _ =>
    }
  }
}

class ConcurrentStartupShardingSpec extends AkkaSpec(ConcurrentStartupShardingSpec.config) with WithLogCapturing {
  import ConcurrentStartupShardingSpec._

  // mute logging of deadLetters
  if (!log.isDebugEnabled)
    system.eventStream.publish(Mute(DeadLettersFilter[Any]))

  // The intended usage is to start sharding in one (or a few) places when the the ActorSystem
  // is started and not to do it concurrently from many threads. However, we can do our best and when using
  // FJP the Await will create additional threads when needed.
  "Concurrent Sharding startup" must {
    "init cluster" in {
      Cluster(system).join(Cluster(system).selfAddress)
      awaitAssert(Cluster(system).selfMember.status should ===(MemberStatus.Up))

      val total = 20
      (1 to total).foreach { n =>
        system.actorOf(Starter.props(n, testActor))
      }

      receiveN(total, 60.seconds)
    }
  }

}
