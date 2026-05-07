/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration._

import language.postfixOps
import org.scalatest.BeforeAndAfterEach

import akka.ConfigurationException
import akka.routing._
import akka.testkit._
import akka.testkit.DefaultTimeout
import akka.testkit.TestEvent._

object ActorConfigurationVerificationSpec {

  class TestActor extends Actor {
    def receive: Receive = {
      case _ =>
    }
  }

  val config = """
    balancing-dispatcher {
      type = "akka.dispatch.BalancingDispatcherConfigurator"
      throughput = 1
    }
    pinned-dispatcher {
      executor = "thread-pool-executor"
      type = PinnedDispatcher
    }
    """
}

class ActorConfigurationVerificationSpec
    extends AkkaSpec(ActorConfigurationVerificationSpec.config)
    with DefaultTimeout
    with BeforeAndAfterEach {
  import ActorConfigurationVerificationSpec._

  override def atStartup(): Unit = {
    system.eventStream.publish(Mute(EventFilter[ConfigurationException]("")))
  }

  "An Actor configured with a BalancingDispatcher" must {
    "fail verification with a ConfigurationException if also configured with a RoundRobinPool" in {
      intercept[ConfigurationException] {
        system.actorOf(RoundRobinPool(2).withDispatcher("balancing-dispatcher").props(Props[TestActor]()))
      }
    }
    "fail verification with a ConfigurationException if also configured with a BroadcastPool" in {
      intercept[ConfigurationException] {
        system.actorOf(BroadcastPool(2).withDispatcher("balancing-dispatcher").props(Props[TestActor]()))
      }
    }
    "fail verification with a ConfigurationException if also configured with a RandomPool" in {
      intercept[ConfigurationException] {
        system.actorOf(RandomPool(2).withDispatcher("balancing-dispatcher").props(Props[TestActor]()))
      }
    }
    "fail verification with a ConfigurationException if also configured with a SmallestMailboxPool" in {
      intercept[ConfigurationException] {
        system.actorOf(SmallestMailboxPool(2).withDispatcher("balancing-dispatcher").props(Props[TestActor]()))
      }
    }
    "fail verification with a ConfigurationException if also configured with a ScatterGatherFirstCompletedPool" in {
      intercept[ConfigurationException] {
        system.actorOf(
          ScatterGatherFirstCompletedPool(nrOfInstances = 2, within = 2 seconds)
            .withDispatcher("balancing-dispatcher")
            .props(Props[TestActor]()))
      }
    }
    "not fail verification with a ConfigurationException also not configured with a Router" in {
      system.actorOf(Props[TestActor]().withDispatcher("balancing-dispatcher"))
    }
  }
  "An Actor configured with a non-balancing dispatcher" must {
    "not fail verification with a ConfigurationException if also configured with a Router" in {
      system.actorOf(RoundRobinPool(2).props(Props[TestActor]().withDispatcher("pinned-dispatcher")))
    }

    "fail verification if the dispatcher cannot be found" in {
      intercept[ConfigurationException] {
        system.actorOf(Props[TestActor]().withDispatcher("does not exist"))
      }
    }

    "fail verification if the dispatcher cannot be found for the head of a router" in {
      intercept[ConfigurationException] {
        system.actorOf(RoundRobinPool(1, routerDispatcher = "does not exist").props(Props[TestActor]()))
      }
    }

    "fail verification if the dispatcher cannot be found for the routees of a router" in {
      intercept[ConfigurationException] {
        system.actorOf(RoundRobinPool(1).props(Props[TestActor]().withDispatcher("does not exist")))
      }
    }
  }
}
