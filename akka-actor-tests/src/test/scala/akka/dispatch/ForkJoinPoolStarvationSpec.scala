/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import com.typesafe.config.ConfigFactory
import akka.actor.{ Actor, Props }
import akka.testkit.{ AkkaSpec, ImplicitSender }
import akka.util.JavaVersion

object ForkJoinPoolStarvationSpec {
  val config = ConfigFactory.parseString("""
      |actorhang {
      |  task-dispatcher {
      |    mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
      |    throughput = 5
      |    fork-join-executor {
      |      parallelism-factor = 2
      |      parallelism-max = 2
      |      parallelism-min = 2
      |    }
      |  }
      |}
    """.stripMargin)

  class SelfBusyActor extends Actor {
    self ! "tick"

    override def receive = {
      case "tick" =>
        self ! "tick"
    }
  }

  class InnocentActor extends Actor {

    override def receive = {
      case "ping" =>
        sender() ! "All fine"
    }
  }

}

class ForkJoinPoolStarvationSpec extends AkkaSpec(ForkJoinPoolStarvationSpec.config) with ImplicitSender {
  import ForkJoinPoolStarvationSpec._

  val Iterations = 1000

  "AkkaForkJoinPool" must {

    "not starve tasks arriving from external dispatchers under high internal traffic" in {
      // TODO issue #31117: starvation with JDK 17 FJP
      if (JavaVersion.majorVersion >= 17)
        pending

      // Two busy actors that will occupy the threads of the dispatcher
      // Since they submit to the local task queue via fork, they can starve external submissions
      system.actorOf(Props(new SelfBusyActor).withDispatcher("actorhang.task-dispatcher"))
      system.actorOf(Props(new SelfBusyActor).withDispatcher("actorhang.task-dispatcher"))

      val innocentActor = system.actorOf(Props(new InnocentActor).withDispatcher("actorhang.task-dispatcher"))

      for (_ <- 1 to Iterations) {
        // External task submission via the default dispatcher
        innocentActor ! "ping"
        expectMsg("All fine")
      }
    }

  }
}
