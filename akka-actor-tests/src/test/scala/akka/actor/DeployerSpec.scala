/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigParseOptions
import language.postfixOps

import akka.routing._
import akka.testkit.AkkaSpec

object DeployerSpec {
  val deployerConf = ConfigFactory.parseString(
    """
      akka.actor.deployment {
        /service1 {
        }
        /service-direct {
          router = from-code
        }
        /service-direct2 {
          router = from-code
          # nr-of-instances ignored when router = from-code
          nr-of-instances = 2
        }
        /service3 {
          dispatcher = my-dispatcher
        }
        /service4 {
          mailbox = my-mailbox
        }
        /service-round-robin {
          router = round-robin-pool
        }
        /service-random {
          router = random-pool
        }
        /service-scatter-gather {
          router = scatter-gather-pool
          within = 2 seconds
        }
        /service-consistent-hashing {
          router = consistent-hashing-pool
        }
        /service-resizer {
          router = round-robin-pool
          resizer {
            lower-bound = 1
            upper-bound = 10
          }
        }
        /some/random-service {
          router = round-robin-pool
        }
        "/some/*" {
          router = random-pool
        }
        "/*/some" {
          router = scatter-gather-pool
        }
        "/double/**" {
          router = random-pool
        }
        "/double/more/**" {
          router = round-robin-pool
        }
      }
      """,
    ConfigParseOptions.defaults)

  class RecipeActor extends Actor {
    def receive = { case _ => }
  }

}

class DeployerSpec extends AkkaSpec(DeployerSpec.deployerConf) {
  "A Deployer" must {

    "be able to parse 'akka.actor.deployment._' with all default values" in {
      val service = "/service1"
      val deployment = system.asInstanceOf[ExtendedActorSystem].provider.deployer.lookup(service.split("/").drop(1))

      deployment should ===(
        Some(
          Deploy(
            service,
            deployment.get.config,
            NoRouter,
            NoScopeGiven,
            Deploy.NoDispatcherGiven,
            Deploy.NoMailboxGiven)))
    }

    "use None deployment for undefined service" in {
      val service = "/undefined"
      val deployment = system.asInstanceOf[ExtendedActorSystem].provider.deployer.lookup(service.split("/").drop(1))
      deployment should ===(None)
    }

    "be able to parse 'akka.actor.deployment._' with dispatcher config" in {
      val service = "/service3"
      val deployment = system.asInstanceOf[ExtendedActorSystem].provider.deployer.lookup(service.split("/").drop(1))

      deployment should ===(
        Some(
          Deploy(
            service,
            deployment.get.config,
            NoRouter,
            NoScopeGiven,
            dispatcher = "my-dispatcher",
            Deploy.NoMailboxGiven)))
    }

    "be able to parse 'akka.actor.deployment._' with mailbox config" in {
      val service = "/service4"
      val deployment = system.asInstanceOf[ExtendedActorSystem].provider.deployer.lookup(service.split("/").drop(1))

      deployment should ===(
        Some(
          Deploy(
            service,
            deployment.get.config,
            NoRouter,
            NoScopeGiven,
            Deploy.NoDispatcherGiven,
            mailbox = "my-mailbox")))
    }

    "detect invalid number-of-instances" in {
      intercept[com.typesafe.config.ConfigException.WrongType] {
        val invalidDeployerConf = ConfigFactory
          .parseString(
            """
            akka.actor.deployment {
              /service-invalid-number-of-instances {
                router = round-robin-pool
                nr-of-instances = boom
              }
            }
            """,
            ConfigParseOptions.defaults)
          .withFallback(AkkaSpec.testConf)

        shutdown(ActorSystem("invalid-number-of-instances", invalidDeployerConf))
      }
    }

    "detect invalid deployment path" in {
      val e = intercept[InvalidActorNameException] {
        val invalidDeployerConf = ConfigFactory
          .parseString(
            """
            akka.actor.deployment {
              /gul/ubåt {
                router = round-robin-pool
                nr-of-instances = 2
              }
            }
            """,
            ConfigParseOptions.defaults)
          .withFallback(AkkaSpec.testConf)

        shutdown(ActorSystem("invalid-path", invalidDeployerConf))
      }
      e.getMessage should include("[ubåt]")
      e.getMessage should include("[/gul/ubåt]")
    }

    "be able to parse 'akka.actor.deployment._' with from-code router" in {
      assertRouting("/service-direct", NoRouter, "/service-direct")
    }

    "ignore nr-of-instances with from-code router" in {
      assertRouting("/service-direct2", NoRouter, "/service-direct2")
    }

    "be able to parse 'akka.actor.deployment._' with round-robin router" in {
      assertRouting("/service-round-robin", RoundRobinPool(1), "/service-round-robin")
    }

    "be able to parse 'akka.actor.deployment._' with random router" in {
      assertRouting("/service-random", RandomPool(1), "/service-random")
    }

    "be able to parse 'akka.actor.deployment._' with scatter-gather router" in {
      assertRouting(
        "/service-scatter-gather",
        ScatterGatherFirstCompletedPool(nrOfInstances = 1, within = 2 seconds),
        "/service-scatter-gather")
    }

    "be able to parse 'akka.actor.deployment._' with consistent-hashing router" in {
      assertRouting("/service-consistent-hashing", ConsistentHashingPool(1), "/service-consistent-hashing")
    }

    "be able to parse 'akka.actor.deployment._' with router resizer" in {
      val resizer = DefaultResizer()
      assertRouting("/service-resizer", RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)), "/service-resizer")
    }

    "be able to use wildcards" in {
      assertRouting("/some/wildcardmatch", RandomPool(1), "/some/*")
      assertRouting(
        "/somewildcardmatch/some",
        ScatterGatherFirstCompletedPool(nrOfInstances = 1, within = 2 seconds),
        "/*/some")
    }

    "be able to use double wildcards" in {
      assertRouting("/double/wildcardmatch", RandomPool(1), "/double/**")
      assertRouting("/double/wildcardmatch/anothermatch", RandomPool(1), "/double/**")
      assertRouting("/double/more/anothermatch", RoundRobinPool(1), "/double/more/**")
      assertNoRouting("/double")
    }

    "have correct router mappings" in {
      val mapping = system.asInstanceOf[ExtendedActorSystem].provider.deployer.routerTypeMapping
      mapping("from-code") should ===(classOf[akka.routing.NoRouter].getName)
      mapping("round-robin-pool") should ===(classOf[akka.routing.RoundRobinPool].getName)
      mapping("round-robin-group") should ===(classOf[akka.routing.RoundRobinGroup].getName)
      mapping("random-pool") should ===(classOf[akka.routing.RandomPool].getName)
      mapping("random-group") should ===(classOf[akka.routing.RandomGroup].getName)
      mapping("balancing-pool") should ===(classOf[akka.routing.BalancingPool].getName)
      mapping("smallest-mailbox-pool") should ===(classOf[akka.routing.SmallestMailboxPool].getName)
      mapping("broadcast-pool") should ===(classOf[akka.routing.BroadcastPool].getName)
      mapping("broadcast-group") should ===(classOf[akka.routing.BroadcastGroup].getName)
      mapping("scatter-gather-pool") should ===(classOf[akka.routing.ScatterGatherFirstCompletedPool].getName)
      mapping("scatter-gather-group") should ===(classOf[akka.routing.ScatterGatherFirstCompletedGroup].getName)
      mapping("consistent-hashing-pool") should ===(classOf[akka.routing.ConsistentHashingPool].getName)
      mapping("consistent-hashing-group") should ===(classOf[akka.routing.ConsistentHashingGroup].getName)
    }

    def assertNoRouting(service: String): Unit = {
      val deployment = system.asInstanceOf[ExtendedActorSystem].provider.deployer.lookup(service.split("/").drop(1))
      deployment shouldNot be(defined)
    }

    def assertRouting(service: String, expected: RouterConfig, expectPath: String): Unit = {
      val deployment = system.asInstanceOf[ExtendedActorSystem].provider.deployer.lookup(service.split("/").drop(1))
      deployment.map(_.path).getOrElse("NOT FOUND") should ===(expectPath)
      deployment.get.routerConfig.getClass should ===(expected.getClass)
      deployment.get.scope should ===(NoScopeGiven)
      expected match {
        case pool: Pool => deployment.get.routerConfig.asInstanceOf[Pool].resizer should ===(pool.resizer)
        case _          =>
      }
    }
  }
}
