/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dispatch

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Await
import scala.concurrent.duration._

import language.postfixOps

import akka.actor.{ Actor, Props }
import akka.pattern.ask
import akka.testkit.AkkaSpec
import akka.testkit.DefaultTimeout

object DispatcherActorSpec {
  val config = """
    test-dispatcher {
    }
    test-throughput-dispatcher {
      throughput = 101
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 1
      }
    }
    test-throughput-deadline-dispatcher {
      throughput = 2
      throughput-deadline-time = 100 milliseconds
      executor = "thread-pool-executor"
      thread-pool-executor {
        fixed-pool-size = 1
      }
    }

    """
  class TestActor extends Actor {
    def receive = {
      case "Hello"   => sender() ! "World"
      case "Failure" => throw new RuntimeException("Expected exception; to test fault-tolerance")
    }
  }

  object OneWayTestActor {
    val oneWay = new CountDownLatch(1)
  }
  class OneWayTestActor extends Actor {
    def receive = {
      case "OneWay" => OneWayTestActor.oneWay.countDown()
    }
  }
}

class DispatcherActorSpec extends AkkaSpec(DispatcherActorSpec.config) with DefaultTimeout {
  import DispatcherActorSpec._

  "A Dispatcher and an Actor" must {

    "support tell" in {
      val actor = system.actorOf(Props[OneWayTestActor]().withDispatcher("test-dispatcher"))
      actor ! "OneWay"
      assert(OneWayTestActor.oneWay.await(1, TimeUnit.SECONDS))
      system.stop(actor)
    }

    "support ask/reply" in {
      val actor = system.actorOf(Props[TestActor]().withDispatcher("test-dispatcher"))
      assert("World" === Await.result(actor ? "Hello", timeout.duration))
      system.stop(actor)
    }

    "respect the throughput setting" in {
      val throughputDispatcher = "test-throughput-dispatcher"

      val works = new AtomicBoolean(true)
      val latch = new CountDownLatch(100)
      val start = new CountDownLatch(1)
      val fastOne = system.actorOf(
        Props(new Actor { def receive = { case "sabotage" => works.set(false) } }).withDispatcher(throughputDispatcher))

      val slowOne = system.actorOf(Props(new Actor {
        def receive = {
          case "hogexecutor" => { sender() ! "OK"; start.await() }
          case "ping"        => if (works.get) latch.countDown()
        }
      }).withDispatcher(throughputDispatcher))

      assert(Await.result(slowOne ? "hogexecutor", timeout.duration) === "OK")
      (1 to 100).foreach { _ =>
        slowOne ! "ping"
      }
      fastOne ! "sabotage"
      start.countDown()
      latch.await(10, TimeUnit.SECONDS)
      system.stop(fastOne)
      system.stop(slowOne)
      assert(latch.getCount() === 0L)
    }

    "respect throughput deadline" in {
      val deadline = 100 millis
      val throughputDispatcher = "test-throughput-deadline-dispatcher"

      val works = new AtomicBoolean(true)
      val latch = new CountDownLatch(1)
      val start = new CountDownLatch(1)
      val ready = new CountDownLatch(1)

      val fastOne = system.actorOf(Props(new Actor {
        def receive = {
          case "ping" => if (works.get) latch.countDown(); context.stop(self)
        }
      }).withDispatcher(throughputDispatcher))

      val slowOne = system.actorOf(Props(new Actor {
        def receive = {
          case "hogexecutor" => { ready.countDown(); start.await() }
          case "ping"        => { works.set(false); context.stop(self) }
        }
      }).withDispatcher(throughputDispatcher))

      slowOne ! "hogexecutor"
      slowOne ! "ping"
      fastOne ! "ping"
      assert(ready.await(2, TimeUnit.SECONDS) === true)
      Thread.sleep(deadline.toMillis + 10) // wait just a bit more than the deadline
      start.countDown()
      assert(latch.await(2, TimeUnit.SECONDS) === true)
    }
  }
}
