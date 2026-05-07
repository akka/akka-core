/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.routing

import scala.concurrent.duration._
import akka.testkit._
import akka.actor.{ Actor, ActorRef, Props }
import akka.actor.Terminated
import akka.routing.FromConfig
import akka.routing.RoundRobinPool
import akka.routing.RandomPool
import akka.routing.RoundRobinGroup
import akka.routing.SmallestMailboxPool
import akka.routing.BroadcastPool
import akka.routing.BroadcastGroup
import akka.routing.ConsistentHashingGroup
import akka.routing.ConsistentHashingPool
import akka.routing.DefaultResizer
import akka.routing.ScatterGatherFirstCompletedGroup
import akka.routing.RandomGroup
import akka.routing.ScatterGatherFirstCompletedPool
import akka.routing.BalancingPool
import akka.routing.TailChoppingGroup
import akka.routing.TailChoppingPool

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

object RouterDocSpec {

  val config = """
#//#config-round-robin-pool
akka.actor.deployment {
  /parent/router1 {
    router = round-robin-pool
    nr-of-instances = 5
  }
}
#//#config-round-robin-pool

#//#config-round-robin-group
akka.actor.deployment {
  /parent/router3 {
    router = round-robin-group
    routees.paths = ["/user/workers/w1", "/user/workers/w2", "/user/workers/w3"]
  }
}
#//#config-round-robin-group

#//#config-random-pool
akka.actor.deployment {
  /parent/router5 {
    router = random-pool
    nr-of-instances = 5
  }
}
#//#config-random-pool

#//#config-random-group
akka.actor.deployment {
  /parent/router7 {
    router = random-group
    routees.paths = ["/user/workers/w1", "/user/workers/w2", "/user/workers/w3"]
  }
}
#//#config-random-group

#//#config-balancing-pool
akka.actor.deployment {
  /parent/router9 {
    router = balancing-pool
    nr-of-instances = 5
  }
}
#//#config-balancing-pool

#//#config-balancing-pool2
akka.actor.deployment {
  /parent/router9b {
    router = balancing-pool
    nr-of-instances = 5
    pool-dispatcher {
      attempt-teamwork = off
    }
  }
}
#//#config-balancing-pool2

#//#config-balancing-pool3
akka.actor.deployment {
  /parent/router10b {
    router = balancing-pool
    nr-of-instances = 5
    pool-dispatcher {
      executor = "thread-pool-executor"

      # allocate exactly 5 threads for this pool
      thread-pool-executor {
        core-pool-size-min = 5
        core-pool-size-max = 5
      }
    }
  }
}
#//#config-balancing-pool3

#//#config-balancing-pool4
akka.actor.deployment {
  /parent/router10c {
    router = balancing-pool
    nr-of-instances = 5
    pool-dispatcher {
      mailbox = myapp.myprioritymailbox
    }
  }
}
#//#config-balancing-pool4

#//#config-smallest-mailbox-pool
akka.actor.deployment {
  /parent/router11 {
    router = smallest-mailbox-pool
    nr-of-instances = 5
  }
}
#//#config-smallest-mailbox-pool

#//#config-broadcast-pool
akka.actor.deployment {
  /parent/router13 {
    router = broadcast-pool
    nr-of-instances = 5
  }
}
#//#config-broadcast-pool

#//#config-broadcast-group
akka.actor.deployment {
  /parent/router15 {
    router = broadcast-group
    routees.paths = ["/user/workers/w1", "/user/workers/w2", "/user/workers/w3"]
  }
}
#//#config-broadcast-group

#//#config-scatter-gather-pool
akka.actor.deployment {
  /parent/router17 {
    router = scatter-gather-pool
    nr-of-instances = 5
    within = 10 seconds
  }
}
#//#config-scatter-gather-pool

#//#config-scatter-gather-group
akka.actor.deployment {
  /parent/router19 {
    router = scatter-gather-group
    routees.paths = ["/user/workers/w1", "/user/workers/w2", "/user/workers/w3"]
    within = 10 seconds
  }
}
#//#config-scatter-gather-group

#//#config-tail-chopping-pool
akka.actor.deployment {
  /parent/router21 {
    router = tail-chopping-pool
    nr-of-instances = 5
    within = 10 seconds
    tail-chopping-router.interval = 20 milliseconds
  }
}
#//#config-tail-chopping-pool

#//#config-tail-chopping-group
akka.actor.deployment {
  /parent/router23 {
    router = tail-chopping-group
    routees.paths = ["/user/workers/w1", "/user/workers/w2", "/user/workers/w3"]
    within = 10 seconds
    tail-chopping-router.interval = 20 milliseconds
  }
}
#//#config-tail-chopping-group

#//#config-consistent-hashing-pool
akka.actor.deployment {
  /parent/router25 {
    router = consistent-hashing-pool
    nr-of-instances = 5
    virtual-nodes-factor = 10
  }
}
#//#config-consistent-hashing-pool

#//#config-consistent-hashing-group
akka.actor.deployment {
  /parent/router27 {
    router = consistent-hashing-group
    routees.paths = ["/user/workers/w1", "/user/workers/w2", "/user/workers/w3"]
    virtual-nodes-factor = 10
  }
}
#//#config-consistent-hashing-group

#//#config-remote-round-robin-pool
akka.actor.deployment {
  /parent/remotePool {
    router = round-robin-pool
    nr-of-instances = 10
    target.nodes = ["akka://app@10.0.0.2:2552", "akka://app@10.0.0.3:2552"]
  }
}
#//#config-remote-round-robin-pool

#//#config-remote-round-robin-pool-artery
akka.actor.deployment {
  /parent/remotePool {
    router = round-robin-pool
    nr-of-instances = 10
    target.nodes = ["tcp://app@10.0.0.2:2552", "akka://app@10.0.0.3:2552"]
  }
}
#//#config-remote-round-robin-pool-artery

#//#config-remote-round-robin-group
akka.actor.deployment {
  /parent/remoteGroup {
    router = round-robin-group
    routees.paths = [
      "akka://app@10.0.0.1:2552/user/workers/w1",
      "akka://app@10.0.0.2:2552/user/workers/w1",
      "akka://app@10.0.0.3:2552/user/workers/w1"]
  }
}
#//#config-remote-round-robin-group

#//#config-remote-round-robin-group-artery
akka.actor.deployment {
  /parent/remoteGroup2 {
    router = round-robin-group
    routees.paths = [
      "akka://app@10.0.0.1:2552/user/workers/w1",
      "akka://app@10.0.0.2:2552/user/workers/w1",
      "akka://app@10.0.0.3:2552/user/workers/w1"]
  }
}
#//#config-remote-round-robin-group-artery

#//#config-resize-pool
akka.actor.deployment {
  /parent/router29 {
    router = round-robin-pool
    resizer {
      lower-bound = 2
      upper-bound = 15
      messages-per-resize = 100
    }
  }
}
#//#config-resize-pool

#//#config-optimal-size-exploring-resize-pool
akka.actor.deployment {
  /parent/router31 {
    router = round-robin-pool
    optimal-size-exploring-resizer {
      enabled = on
      action-interval = 5s
      downsize-after-underutilized-for = 72h
    }
  }
}
#//#config-optimal-size-exploring-resize-pool

#//#config-pool-dispatcher
akka.actor.deployment {
  /poolWithDispatcher {
    router = random-pool
    nr-of-instances = 5
    pool-dispatcher {
      fork-join-executor.parallelism-min = 5
      fork-join-executor.parallelism-max = 5
    }
  }
}
#//#config-pool-dispatcher

router-dispatcher {}
"""

  final case class Work(payload: String)

  //#router-in-actor
  import akka.routing.{ ActorRefRoutee, RoundRobinRoutingLogic, Router }

  class Master extends Actor {
    var router = {
      val routees = Vector.fill(5) {
        val r = context.actorOf(Props[Worker]())
        context.watch(r)
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    def receive = {
      case w: Work =>
        router.route(w, sender())
      case Terminated(a) =>
        router = router.removeRoutee(a)
        val r = context.actorOf(Props[Worker]())
        context.watch(r)
        router = router.addRoutee(r)
    }
  }
  //#router-in-actor

  class Worker extends Actor {
    def receive = {
      case _ =>
    }
  }

  //#create-worker-actors
  class Workers extends Actor {
    context.actorOf(Props[Worker](), name = "w1")
    context.actorOf(Props[Worker](), name = "w2")
    context.actorOf(Props[Worker](), name = "w3")
    // ...
    //#create-worker-actors

    def receive = {
      case _ =>
    }
  }

  class Parent extends Actor {

    //#paths
    val paths = List("/user/workers/w1", "/user/workers/w2", "/user/workers/w3")
    //#paths

    //#round-robin-pool-1
    val router1: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router1")
    //#round-robin-pool-1

    //#round-robin-pool-2
    val router2: ActorRef =
      context.actorOf(RoundRobinPool(5).props(Props[Worker]()), "router2")
    //#round-robin-pool-2

    //#round-robin-group-1
    val router3: ActorRef =
      context.actorOf(FromConfig.props(), "router3")
    //#round-robin-group-1

    //#round-robin-group-2
    val router4: ActorRef =
      context.actorOf(RoundRobinGroup(paths).props(), "router4")
    //#round-robin-group-2

    //#random-pool-1
    val router5: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router5")
    //#random-pool-1

    //#random-pool-2
    val router6: ActorRef =
      context.actorOf(RandomPool(5).props(Props[Worker]()), "router6")
    //#random-pool-2

    //#random-group-1
    val router7: ActorRef =
      context.actorOf(FromConfig.props(), "router7")
    //#random-group-1

    //#random-group-2
    val router8: ActorRef =
      context.actorOf(RandomGroup(paths).props(), "router8")
    //#random-group-2

    //#balancing-pool-1
    val router9: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router9")
    //#balancing-pool-1

    //#balancing-pool-2
    val router10: ActorRef =
      context.actorOf(BalancingPool(5).props(Props[Worker]()), "router10")
    //#balancing-pool-2

    // #balancing-pool-3
    val router10b: ActorRef =
      context.actorOf(BalancingPool(20).props(Props[Worker]()), "router10b")
    //#balancing-pool-3
    for (i <- 1 to 100) router10b ! i
    val threads10b = Thread.getAllStackTraces.keySet.asScala.filter { _.getName contains "router10b" }
    val threads10bNr = threads10b.size
    require(
      threads10bNr == 5,
      s"Expected 5 threads for router10b, had $threads10bNr! Got: ${threads10b.map(_.getName)}")

    //#smallest-mailbox-pool-1
    val router11: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router11")
    //#smallest-mailbox-pool-1

    //#smallest-mailbox-pool-2
    val router12: ActorRef =
      context.actorOf(SmallestMailboxPool(5).props(Props[Worker]()), "router12")
    //#smallest-mailbox-pool-2

    //#broadcast-pool-1
    val router13: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router13")
    //#broadcast-pool-1

    //#broadcast-pool-2
    val router14: ActorRef =
      context.actorOf(BroadcastPool(5).props(Props[Worker]()), "router14")
    //#broadcast-pool-2

    //#broadcast-group-1
    val router15: ActorRef =
      context.actorOf(FromConfig.props(), "router15")
    //#broadcast-group-1

    //#broadcast-group-2
    val router16: ActorRef =
      context.actorOf(BroadcastGroup(paths).props(), "router16")
    //#broadcast-group-2

    //#scatter-gather-pool-1
    val router17: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router17")
    //#scatter-gather-pool-1

    //#scatter-gather-pool-2
    val router18: ActorRef =
      context.actorOf(ScatterGatherFirstCompletedPool(5, within = 10.seconds).props(Props[Worker]()), "router18")
    //#scatter-gather-pool-2

    //#scatter-gather-group-1
    val router19: ActorRef =
      context.actorOf(FromConfig.props(), "router19")
    //#scatter-gather-group-1

    //#scatter-gather-group-2
    val router20: ActorRef =
      context.actorOf(ScatterGatherFirstCompletedGroup(paths, within = 10.seconds).props(), "router20")
    //#scatter-gather-group-2

    //#tail-chopping-pool-1
    val router21: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router21")
    //#tail-chopping-pool-1

    //#tail-chopping-pool-2
    val router22: ActorRef =
      context.actorOf(TailChoppingPool(5, within = 10.seconds, interval = 20.millis).props(Props[Worker]()), "router22")
    //#tail-chopping-pool-2

    //#tail-chopping-group-1
    val router23: ActorRef =
      context.actorOf(FromConfig.props(), "router23")
    //#tail-chopping-group-1

    //#tail-chopping-group-2
    val router24: ActorRef =
      context.actorOf(TailChoppingGroup(paths, within = 10.seconds, interval = 20.millis).props(), "router24")
    //#tail-chopping-group-2

    //#consistent-hashing-pool-1
    val router25: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router25")
    //#consistent-hashing-pool-1

    //#consistent-hashing-pool-2
    val router26: ActorRef =
      context.actorOf(ConsistentHashingPool(5).props(Props[Worker]()), "router26")
    //#consistent-hashing-pool-2

    //#consistent-hashing-group-1
    val router27: ActorRef =
      context.actorOf(FromConfig.props(), "router27")
    //#consistent-hashing-group-1

    //#consistent-hashing-group-2
    val router28: ActorRef =
      context.actorOf(ConsistentHashingGroup(paths).props(), "router28")
    //#consistent-hashing-group-2

    //#resize-pool-1
    val router29: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router29")
    //#resize-pool-1

    //#resize-pool-2
    val resizer = DefaultResizer(lowerBound = 2, upperBound = 15)
    val router30: ActorRef =
      context.actorOf(RoundRobinPool(5, Some(resizer)).props(Props[Worker]()), "router30")
    //#resize-pool-2

    //#optimal-size-exploring-resize-pool
    val router31: ActorRef =
      context.actorOf(FromConfig.props(Props[Worker]()), "router31")
    //#optimal-size-exploring-resize-pool

    def receive = {
      case _ =>
    }

  }

  class Echo extends Actor {
    def receive = {
      case m => sender() ! m
    }
  }
}

@nowarn("msg=never used") // sample snippets
class RouterDocSpec extends AkkaSpec(RouterDocSpec.config) with ImplicitSender {

  import RouterDocSpec._

  //#create-workers
  system.actorOf(Props[Workers](), "workers")
  //#create-workers

  //#create-parent
  system.actorOf(Props[Parent](), "parent")
  //#create-parent

  "demonstrate dispatcher" in {
    //#dispatchers
    val router: ActorRef = system.actorOf(
      // “head” router actor will run on "router-dispatcher" dispatcher
      // Worker routees will run on "pool-dispatcher" dispatcher
      RandomPool(5, routerDispatcher = "router-dispatcher").props(Props[Worker]()),
      name = "poolWithDispatcher")
    //#dispatchers
  }

  "demonstrate broadcast" in {
    val router = system.actorOf(RoundRobinPool(nrOfInstances = 5).props(Props[Echo]()))
    //#broadcastDavyJonesWarning
    import akka.routing.Broadcast
    router ! Broadcast("Watch out for Davy Jones' locker")
    //#broadcastDavyJonesWarning
    (receiveN(5, 5.seconds.dilated) should have).length(5)
  }

  "demonstrate PoisonPill" in {
    val router = watch(system.actorOf(RoundRobinPool(nrOfInstances = 5).props(Props[Echo]())))
    //#poisonPill
    import akka.actor.PoisonPill
    router ! PoisonPill
    //#poisonPill
    expectTerminated(router)
  }

  "demonstrate broadcast of PoisonPill" in {
    val router = watch(system.actorOf(RoundRobinPool(nrOfInstances = 5).props(Props[Echo]())))
    //#broadcastPoisonPill
    import akka.actor.PoisonPill
    import akka.routing.Broadcast
    router ! Broadcast(PoisonPill)
    //#broadcastPoisonPill
    expectTerminated(router)
  }

  "demonstrate Kill" in {
    val router = watch(system.actorOf(RoundRobinPool(nrOfInstances = 5).props(Props[Echo]())))
    //#kill
    import akka.actor.Kill
    router ! Kill
    //#kill
    expectTerminated(router)
  }

  "demonstrate broadcast of Kill" in {
    val router = watch(system.actorOf(RoundRobinPool(nrOfInstances = 5).props(Props[Echo]())))
    //#broadcastKill
    import akka.actor.Kill
    import akka.routing.Broadcast
    router ! Broadcast(Kill)
    //#broadcastKill
    expectTerminated(router)
  }

  "demonstrate remote deploy" in {
    //#remoteRoutees
    import akka.actor.{ Address, AddressFromURIString }
    import akka.remote.routing.RemoteRouterConfig
    val addresses =
      Seq(Address("akka", "remotesys", "otherhost", 1234), AddressFromURIString("akka://othersys@anotherhost:1234"))
    val routerRemote = system.actorOf(RemoteRouterConfig(RoundRobinPool(5), addresses).props(Props[Echo]()))
    //#remoteRoutees
  }

  // only compile test
  def demonstrateRemoteDeployWithArtery(): Unit = {
    //#remoteRoutees-artery
    import akka.actor.{ Address, AddressFromURIString }
    import akka.remote.routing.RemoteRouterConfig
    val addresses =
      Seq(Address("akka", "remotesys", "otherhost", 1234), AddressFromURIString("akka://othersys@anotherhost:1234"))
    val routerRemote = system.actorOf(RemoteRouterConfig(RoundRobinPool(5), addresses).props(Props[Echo]()))
    //#remoteRoutees-artery
  }
}
