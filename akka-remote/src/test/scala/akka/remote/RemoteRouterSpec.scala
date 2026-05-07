/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.collection.immutable

import com.typesafe.config._

import akka.actor._
import akka.remote.routing._
import akka.routing._
import akka.testkit._
import akka.testkit.TestActors.echoActorProps

object RemoteRouterSpec {
  class Parent extends Actor {
    def receive = {
      case (p: Props, name: String) =>
        sender() ! context.actorOf(p, name)
    }
  }
}

class RemoteRouterSpec extends AkkaSpec("""
    akka.actor.provider = remote
    akka.remote.use-unsafe-remote-features-outside-cluster = on
    akka.remote.artery.canonical {
      hostname = "localhost"
      port = 0
    }
    akka.actor.deployment {
      /remote-override {
        router = round-robin-pool
        nr-of-instances = 4
      }
      /round {
        router = round-robin-pool
        nr-of-instances = 5
      }
      /sys-parent/round {
        router = round-robin-pool
        nr-of-instances = 6
      }
    }""") {

  import RemoteRouterSpec._

  val port = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
  val sysName = system.name
  val masterSystemName = "Master" + sysName
  val protocol = "akka"
  val conf = ConfigFactory.parseString(s"""
    akka {
      actor.deployment {
        /blub {
          router = round-robin-pool
          nr-of-instances = 2
          target.nodes = ["$protocol://${sysName}@localhost:${port}"]
        }
        /elastic-blub {
          router = round-robin-pool
          resizer {
            lower-bound = 2
            upper-bound = 3
          }
          target.nodes = ["$protocol://${sysName}@localhost:${port}"]
        }
        /remote-blub {
          remote = "$protocol://${sysName}@localhost:${port}"
          router = round-robin-pool
          nr-of-instances = 2
        }
        /local-blub {
          remote = "akka://$masterSystemName"
          router = round-robin-pool
          nr-of-instances = 2
          target.nodes = ["$protocol://${sysName}@localhost:${port}"]
        }
        /local-blub2 {
          router = round-robin-pool
          nr-of-instances = 4
          target.nodes = ["$protocol://${sysName}@localhost:${port}"]
        }
      }
    }""").withFallback(system.settings.config)
  val masterSystem = ActorSystem(masterSystemName, conf)

  override def afterTermination(): Unit = {
    shutdown(masterSystem)
  }

  def collectRouteePaths(probe: TestProbe, router: ActorRef, n: Int): immutable.Seq[ActorPath] = {
    for (i <- 1 to n) yield {
      val msg = i.toString
      router.tell(msg, probe.ref)
      probe.expectMsg(msg)
      probe.lastSender.path
    }
  }

  "A Remote Router" must {

    "deploy its children on remote host driven by configuration" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(RoundRobinPool(2).props(echoActorProps), "blub")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 2
      children.map(_.parent) should have size 1
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "deploy its children on remote host driven by programmatic definition" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(
        new RemoteRouterConfig(RoundRobinPool(2), Seq(Address(protocol, sysName, "localhost", port)))
          .props(echoActorProps),
        "blub2")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 2
      children.map(_.parent) should have size 1
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "deploy dynamic resizable number of children on remote host driven by configuration" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(FromConfig.props(echoActorProps), "elastic-blub")
      val replies = collectRouteePaths(probe, router, 5000)
      val children = replies.toSet
      children.size should be >= 2
      children.map(_.parent) should have size 1
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "deploy remote routers based on configuration" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(FromConfig.props(echoActorProps), "remote-blub")
      router.path.address.toString should ===(s"$protocol://${sysName}@localhost:${port}")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 2
      val parents = children.map(_.parent)
      parents should have size 1
      parents.head should ===(router.path)
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "deploy remote routers based on explicit deployment" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(
        RoundRobinPool(2)
          .props(echoActorProps)
          .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(s"$protocol://${sysName}@localhost:${port}")))),
        "remote-blub2")
      router.path.address.toString should ===(s"$protocol://${sysName}@localhost:${port}")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 2
      val parents = children.map(_.parent)
      parents should have size 1
      parents.head should ===(router.path)
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "let remote deployment be overridden by local configuration" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(
        RoundRobinPool(2)
          .props(echoActorProps)
          .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(s"$protocol://${sysName}@localhost:${port}")))),
        "local-blub")
      router.path.address.toString should ===(s"akka://$masterSystemName")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 2
      val parents = children.map(_.parent)
      parents should have size 1
      parents.head.address should ===(Address(protocol, sysName, "localhost", port))
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "let remote deployment router be overridden by local configuration" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(
        RoundRobinPool(2)
          .props(echoActorProps)
          .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(s"$protocol://${sysName}@localhost:${port}")))),
        "local-blub2")
      router.path.address.toString should ===(s"$protocol://${sysName}@localhost:${port}")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 4
      val parents = children.map(_.parent)
      parents should have size 1
      parents.head should ===(router.path)
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "let remote deployment be overridden by remote configuration" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(
        RoundRobinPool(2)
          .props(echoActorProps)
          .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(s"$protocol://${sysName}@localhost:${port}")))),
        "remote-override")
      router.path.address.toString should ===(s"$protocol://${sysName}@localhost:${port}")
      val replies = collectRouteePaths(probe, router, 5)
      val children = replies.toSet
      children should have size 4
      val parents = children.map(_.parent)
      parents should have size 1
      parents.head should ===(router.path)
      children.foreach(_.address.toString should ===(s"$protocol://${sysName}@localhost:${port}"))
      masterSystem.stop(router)
    }

    "set supplied supervisorStrategy" in {
      val probe = TestProbe()(masterSystem)
      val escalator = OneForOneStrategy() {
        case e => probe.ref ! e; SupervisorStrategy.Escalate
      }
      val router = masterSystem.actorOf(
        new RemoteRouterConfig(
          RoundRobinPool(1, supervisorStrategy = escalator),
          Seq(Address(protocol, sysName, "localhost", port))).props(Props.empty),
        "blub3")

      router.tell(GetRoutees, probe.ref)
      EventFilter[ActorKilledException](occurrences = 1).intercept {
        probe.expectMsgType[Routees].routees.head.send(Kill, testActor)
      }(masterSystem)
      probe.expectMsgType[ActorKilledException]
    }

    "load settings from config for local router" in {
      val probe = TestProbe()(masterSystem)
      val router = masterSystem.actorOf(FromConfig.props(echoActorProps), "round")
      val replies = collectRouteePaths(probe, router, 10)
      val children = replies.toSet
      children should have size 5
      masterSystem.stop(router)
    }

    "load settings from config for local child router of system actor" in {
      // we don't really support deployment configuration of system actors, but
      // it's used for the pool of the SimpleDnsManager "/IO-DNS/inet-address"
      val probe = TestProbe()(masterSystem)
      val parent = masterSystem.asInstanceOf[ExtendedActorSystem].systemActorOf(Props[Parent](), "sys-parent")
      parent.tell((FromConfig.props(echoActorProps), "round"), probe.ref)
      val router = probe.expectMsgType[ActorRef]
      val replies = collectRouteePaths(probe, router, 10)
      val children = replies.toSet
      children should have size 6
      masterSystem.stop(router)
    }

  }

}
