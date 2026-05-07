/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.routing

import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.actor.Actor
import akka.actor.Props
import CustomRouterDocSpec.RedundancyRoutingLogic

import scala.collection.immutable
import akka.actor.ActorSystem
import akka.routing.FromConfig
import akka.actor.ActorRef

object CustomRouterDocSpec {

  val config = """
#//#config
akka.actor.deployment {
  /redundancy2 {
    router = "jdocs.routing.RedundancyGroup"
    routees.paths = ["/user/s1", "/user/s2", "/user/s3"]
    nbr-copies = 5
  }
}
#//#config
"""

  val jconfig = """
#//#jconfig
akka.actor.deployment {
  /redundancy2 {
    router = "jdocs.routing.RedundancyGroup"
    routees.paths = ["/user/s1", "/user/s2", "/user/s3"]
    nbr-copies = 5
  }
}
#//#jconfig
"""

  //#routing-logic
  import scala.collection.immutable
  import akka.routing.RoundRobinRoutingLogic
  import akka.routing.RoutingLogic
  import akka.routing.Routee
  import akka.routing.SeveralRoutees

  class RedundancyRoutingLogic(nbrCopies: Int) extends RoutingLogic {
    val roundRobin = RoundRobinRoutingLogic()
    def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
      val targets = (1 to nbrCopies).map(_ => roundRobin.select(message, routees))
      SeveralRoutees(targets)
    }
  }
  //#routing-logic

  class Storage extends Actor {
    def receive = {
      case x => sender() ! x
    }
  }

  //#unit-test-logic
  final case class TestRoutee(n: Int) extends Routee {
    override def send(message: Any, sender: ActorRef): Unit = ()
  }

  //#unit-test-logic
}

//#group
import akka.dispatch.Dispatchers
import akka.routing.Group
import akka.routing.Router
import akka.japi.Util.immutableSeq
import com.typesafe.config.Config

final case class RedundancyGroup(routeePaths: immutable.Iterable[String], nbrCopies: Int) extends Group {

  def this(config: Config) =
    this(routeePaths = immutableSeq(config.getStringList("routees.paths")), nbrCopies = config.getInt("nbr-copies"))

  override def paths(system: ActorSystem): immutable.Iterable[String] = routeePaths

  override def createRouter(system: ActorSystem): Router =
    new Router(new RedundancyRoutingLogic(nbrCopies))

  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId
}
//#group

class CustomRouterDocSpec extends AkkaSpec(CustomRouterDocSpec.config) with ImplicitSender {

  import CustomRouterDocSpec._
  import akka.routing.SeveralRoutees

  "unit test routing logic" in {
    //#unit-test-logic
    val logic = new RedundancyRoutingLogic(nbrCopies = 3)

    val routees = for (n <- 1 to 7) yield TestRoutee(n)

    val r1 = logic.select("msg", routees)
    r1.asInstanceOf[SeveralRoutees].routees should be(Vector(TestRoutee(1), TestRoutee(2), TestRoutee(3)))

    val r2 = logic.select("msg", routees)
    r2.asInstanceOf[SeveralRoutees].routees should be(Vector(TestRoutee(4), TestRoutee(5), TestRoutee(6)))

    val r3 = logic.select("msg", routees)
    r3.asInstanceOf[SeveralRoutees].routees should be(Vector(TestRoutee(7), TestRoutee(1), TestRoutee(2)))
    //#unit-test-logic

  }

  "demonstrate usage of custom router" in {
    //#usage-1
    for (n <- 1 to 10) system.actorOf(Props[Storage](), "s" + n)

    val paths = for (n <- 1 to 10) yield ("/user/s" + n)
    val redundancy1: ActorRef =
      system.actorOf(RedundancyGroup(paths, nbrCopies = 3).props(), name = "redundancy1")
    redundancy1 ! "important"
    //#usage-1

    for (_ <- 1 to 3) expectMsg("important")

    //#usage-2
    val redundancy2: ActorRef = system.actorOf(FromConfig.props(), name = "redundancy2")
    redundancy2 ! "very important"
    //#usage-2

    for (_ <- 1 to 5) expectMsg("very important")

  }

}
