/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }
import language.postfixOps

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.testkit._
import akka.testkit.TestEvent._

object ResizerSpec {

  val config = """
    akka.actor.deployment {
      /router1 {
        router = round-robin-pool
        resizer {
          lower-bound = 2
          upper-bound = 3
        }
      }
    }
    """

  class TestActor extends Actor {
    def receive = {
      case latch: TestLatch => latch.countDown()
    }
  }

}

class ResizerSpec extends AkkaSpec(ResizerSpec.config) with DefaultTimeout with ImplicitSender {

  import akka.routing.ResizerSpec._

  override def atStartup(): Unit = {
    // when shutting down some Resize messages might hang around
    system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*Resize")))
  }

  def routeeSize(router: ActorRef): Int =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees.size

  "Resizer fromConfig" must {
    def parseCfg(cfgString: String): Config = {
      val referenceCfg = ConfigFactory.defaultReference(ActorSystem.findClassLoader())
      ConfigFactory.parseString(cfgString).withFallback(referenceCfg.getConfig("akka.actor.deployment.default"))
    }

    "load DefaultResizer from config when resizer is enabled" in {
      val cfg = parseCfg("""
        resizer {
          enabled = on
        }
        """)
      Resizer.fromConfig(cfg).get shouldBe a[DefaultResizer]
    }

    "load MetricsBasedResizer from config when optimal-size-exploring-resizer is enabled" in {
      val cfg = parseCfg("""
        optimal-size-exploring-resizer {
          enabled = on
        }
        """)
      Resizer.fromConfig(cfg).get shouldBe a[DefaultOptimalSizeExploringResizer]
    }

    "throws exception when both resizer and optimal-size-exploring-resizer is enabled" in {
      val cfg = parseCfg("""
        optimal-size-exploring-resizer {
          enabled = on
        }
        resizer {
          enabled = on
        }
      """)
      intercept[ResizerInitializationException] {
        Resizer.fromConfig(cfg)
      }
    }

    "return None if neither resizer is enabled which is default" in {
      Resizer.fromConfig(parseCfg("")) shouldBe empty
    }
  }

  "DefaultResizer" must {

    "use settings to evaluate capacity" in {
      val resizer = DefaultResizer(lowerBound = 2, upperBound = 3)

      val c1 = resizer.capacity(Vector.empty[Routee])
      c1 should ===(2)

      val current =
        Vector(ActorRefRoutee(system.actorOf(Props[TestActor]())), ActorRefRoutee(system.actorOf(Props[TestActor]())))
      val c2 = resizer.capacity(current)
      c2 should ===(0)
    }

    "use settings to evaluate rampUp" in {
      val resizer = DefaultResizer(lowerBound = 2, upperBound = 10, rampupRate = 0.2)

      resizer.rampup(pressure = 9, capacity = 10) should ===(0)
      resizer.rampup(pressure = 5, capacity = 5) should ===(1)
      resizer.rampup(pressure = 6, capacity = 6) should ===(2)
    }

    "use settings to evaluate backoff" in {
      val resizer = DefaultResizer(lowerBound = 2, upperBound = 10, backoffThreshold = 0.3, backoffRate = 0.1)

      resizer.backoff(pressure = 10, capacity = 10) should ===(0)
      resizer.backoff(pressure = 4, capacity = 10) should ===(0)
      resizer.backoff(pressure = 3, capacity = 10) should ===(0)
      resizer.backoff(pressure = 2, capacity = 10) should ===(-1)
      resizer.backoff(pressure = 0, capacity = 10) should ===(-1)
      resizer.backoff(pressure = 1, capacity = 9) should ===(-1)
      resizer.backoff(pressure = 0, capacity = 9) should ===(-1)
    }

    "be possible to define programmatically" in {
      val latch = new TestLatch(3)

      val resizer = DefaultResizer(lowerBound = 2, upperBound = 3)
      val router = system.actorOf(RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)).props(Props[TestActor]()))

      router ! latch
      router ! latch
      router ! latch

      Await.ready(latch, remainingOrDefault)

      // messagesPerResize is 10 so there is no risk of additional resize
      routeeSize(router) should ===(2)
    }

    "be possible to define in configuration" in {
      val latch = new TestLatch(3)

      val router = system.actorOf(FromConfig.props(Props[TestActor]()), "router1")

      router ! latch
      router ! latch
      router ! latch

      Await.ready(latch, remainingOrDefault)

      routeeSize(router) should ===(2)
    }

    "grow as needed under pressure" in {
      // make sure the pool starts at the expected lower limit and grows to the upper as needed
      // as influenced by the backlog of blocking pooled actors

      val resizer = DefaultResizer(
        lowerBound = 3,
        upperBound = 5,
        rampupRate = 0.1,
        backoffRate = 0.0,
        pressureThreshold = 1,
        messagesPerResize = 1,
        backoffThreshold = 0.0)

      val router = system.actorOf(RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)).props(Props(new Actor {
        def receive = {
          case d: FiniteDuration =>
            Thread.sleep(d.dilated.toMillis); sender() ! "done"
          case "echo" => sender() ! "reply"
        }
      })))

      // first message should create the minimum number of routees
      router ! "echo"
      expectMsg("reply")

      routeeSize(router) should ===(resizer.lowerBound)

      def loop(loops: Int, d: FiniteDuration) = {
        for (_ <- 0 until loops) {
          router ! d
          // sending in too quickly will result in skipped resize due to many resizeInProgress conflicts
          Thread.sleep(20.millis.dilated.toMillis)
        }
        within((d * loops / resizer.lowerBound) + 2.seconds.dilated) {
          for (_ <- 0 until loops) expectMsg("done")
        }
      }

      // 2 more should go through without triggering more
      loop(2, 200 millis)
      routeeSize(router) should ===(resizer.lowerBound)

      // a whole bunch should max it out
      loop(20, 500 millis)
      routeeSize(router) should ===(resizer.upperBound)
    }

    "backoff" in within(10 seconds) {
      val resizer = DefaultResizer(
        lowerBound = 2,
        upperBound = 5,
        rampupRate = 1.0,
        backoffRate = 1.0,
        backoffThreshold = 0.40,
        pressureThreshold = 1,
        messagesPerResize = 2)

      val router = system.actorOf(RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)).props(Props(new Actor {
        def receive = {
          case n: Int if n <= 0 => // done
          case n: Int           => Thread.sleep((n millis).dilated.toMillis)
        }
      })))

      // put some pressure on the router
      for (_ <- 0 until 15) {
        router ! 150
        Thread.sleep((20 millis).dilated.toMillis)
      }

      val z = routeeSize(router)
      z should be > (2)

      Thread.sleep((300 millis).dilated.toMillis)

      // let it cool down
      awaitCond({
        router ! 0 // trigger resize
        Thread.sleep((20 millis).dilated.toMillis)
        routeeSize(router) < z
      }, interval = 500.millis.dilated)

    }

  }

}
