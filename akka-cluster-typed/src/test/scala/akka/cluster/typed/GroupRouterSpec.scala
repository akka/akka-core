/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.GroupRouter
import akka.actor.typed.scaladsl.Routers
import akka.serialization.jackson.CborSerializable

object GroupRouterSpec {
  def config = ConfigFactory.parseString("""
    akka {
      loglevel = debug
      actor.provider = cluster
      remote.artery {
        canonical {
          hostname = 127.0.0.1
          port = 0
        }
      }
    }
  """)

  object PingActor {
    case class Pong(workerActor: ActorRef[_]) extends CborSerializable
    case class Ping(replyTo: ActorRef[Pong]) extends CborSerializable

    def apply(): Behavior[Ping] = Behaviors.receive {
      case (ctx, Ping(replyTo)) =>
        replyTo ! Pong(ctx.self)
        Behaviors.same
    }
  }

  object Pinger {
    sealed trait Command
    private case class SawPong(worker: ActorRef[_]) extends Command
    case class DonePinging(pongs: Int, uniqueWorkers: Set[ActorRef[_]])
    def apply(
        router: ActorRef[PingActor.Ping],
        requestedPings: Int,
        tellMeWhenDone: ActorRef[DonePinging]): Behavior[Command] =
      Behaviors.setup { ctx =>
        val pongAdapter = ctx.messageAdapter[PingActor.Pong] {
          case PingActor.Pong(ref) => SawPong(ref)
        }
        (0 to requestedPings).foreach(_ => router ! PingActor.Ping(pongAdapter))
        var pongs = 0
        var uniqueWorkers = Set.empty[ActorRef[_]]

        Behaviors.receiveMessage {
          case SawPong(worker) =>
            pongs += 1
            uniqueWorkers += worker
            if (pongs >= requestedPings) {
              tellMeWhenDone ! DonePinging(pongs, uniqueWorkers)
              Behaviors.stopped
            } else {
              Behaviors.same
            }
        }
      }
  }

  case class GroupRouterSpecSettings(node1WorkerCount: Int, node2WorkerCount: Int, messageCount: Int)

  val pingPongKey = ServiceKey[PingActor.Ping]("ping-pong")

  case class Result(actorsInSystem1: Int, actorsInSystem2: Int)

}

class GroupRouterSpec extends ScalaTestWithActorTestKit(GroupRouterSpec.config) with AnyWordSpecLike with LogCapturing {
  import GroupRouterSpec._

  /**
   * Starts a new pair of nodes, forms a cluster, runs a ping-pong session and hands the result to the test for verification
   */
  def checkGroupRouterBehavior(groupRouter: GroupRouter[PingActor.Ping], settings: GroupRouterSpecSettings): Result = {

    val resultProbe = testKit.createTestProbe[Pinger.DonePinging]()

    val system1 = ActorSystem(Behaviors.setup[Receptionist.Listing] {
      ctx =>
        (0 until settings.node1WorkerCount).foreach { i =>
          val worker = ctx.spawn(PingActor(), s"ping-pong-$i")
          ctx.system.receptionist ! Receptionist.Register(pingPongKey, worker)
        }
        ctx.system.receptionist ! Receptionist.Subscribe(pingPongKey, ctx.self)
        Behaviors.receiveMessage {
          case pingPongKey.Listing(update) if update.size == settings.node1WorkerCount + settings.node2WorkerCount =>
            // the requested number of workers are started and registered with the receptionist
            // a new router will see all after this has been observed
            ctx.log.debug("Saw {} workers, starting router and pinger", update.size)
            val router = ctx.spawn(groupRouter, "group-router")
            ctx.spawn(Pinger(router, settings.messageCount, resultProbe.ref), "pinger")
            // ignore further listings
            Behaviors.empty
          case _ =>
            Behaviors.same
        }
    }, system.name, config)

    val system2 = ActorSystem(Behaviors.setup[Unit] { ctx =>
      (0 until settings.node2WorkerCount).foreach { i =>
        val worker = ctx.spawn(PingActor(), s"ping-pong-$i")
        ctx.system.receptionist ! Receptionist.Register(pingPongKey, worker)
      }
      Behaviors.empty
    }, system.name, config)

    try {
      val node1 = Cluster(system1)
      node1.manager ! Join(node1.selfMember.address)
      val node2 = Cluster(system2)
      node2.manager ! Join(node1.selfMember.address)

      val donePinging = resultProbe.receiveMessage(10.seconds)
      if (donePinging.pongs < settings.messageCount) {
        fail(s"Expected ${settings.messageCount} ping-pongs but only saw ${donePinging.pongs}")
      }
      // pinger runs in system 1 so those workers are local
      val (workersInAddress1, workersInAddress2) = donePinging.uniqueWorkers.partition(_.path.address.hasLocalScope)
      Result(workersInAddress1.size, workersInAddress2.size)
    } finally {
      system1.terminate()
      system2.terminate()
    }
  }
  "GroupRouter" must {
    // default is to not preferLocalRoutees
    List(
      "random" -> Routers.group(pingPongKey).withRandomRouting(), // default group is same as this
      "round robin" -> Routers.group(pingPongKey).withRoundRobinRouting()).foreach {
      case (strategy, groupRouter) =>
        s"use all reachable routees if preferLocalRoutees is not enabled, strategy $strategy" in {
          val settings = GroupRouterSpecSettings(node1WorkerCount = 2, node2WorkerCount = 2, messageCount = 100)
          val result = checkGroupRouterBehavior(groupRouter, settings)
          result.actorsInSystem1 should ===(settings.node1WorkerCount)
          result.actorsInSystem2 should ===(settings.node2WorkerCount)
        }
    }

    List(
      "random" -> Routers.group(pingPongKey).withRandomRouting(true),
      "round robin" -> Routers.group(pingPongKey).withRoundRobinRouting(true)).foreach {
      case (strategy, groupRouter) =>
        s"only use local routees if preferLocalRoutees is enabled and there are local routees, strategy $strategy" in {
          val settings = GroupRouterSpecSettings(node1WorkerCount = 2, node2WorkerCount = 2, messageCount = 100)
          val result = checkGroupRouterBehavior(groupRouter, settings)
          result.actorsInSystem1 should ===(settings.node1WorkerCount)
          result.actorsInSystem2 should ===(0)
        }

        s"use remote routees if preferLocalRoutees is enabled but there is no local routees, strategy $strategy" in {
          val settings = GroupRouterSpecSettings(node1WorkerCount = 0, node2WorkerCount = 2, messageCount = 100)
          val result = checkGroupRouterBehavior(groupRouter, settings)
          result.actorsInSystem1 should ===(0)
          result.actorsInSystem2 should ===(settings.node2WorkerCount)
        }
    }
  }
}
