/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.net.URLEncoder
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfterEach

import akka.actor.{ Actor, ActorLogging, ActorRef, PoisonPill, Props }
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestLatch
import akka.testkit.TestProbe

object BalancingSpec {
  val counter = new AtomicInteger(1)

  class Worker(latch: TestLatch, startOthers: Future[Unit]) extends Actor with ActorLogging {
    lazy val id = counter.getAndIncrement()
    log.debug("Worker started")

    def receive = {
      case _: Int =>
        latch.countDown()
        if (id == 1) {
          if (!latch.isOpen) {
            log.debug("Waiting for all routees to receieve a message")
            // wait for all routees to receive a message before processing
            Await.result(latch, 1.minute)
            log.debug("All routees receieved a message, continuing")
          }
        } else {
          if (!startOthers.isCompleted) {
            log.debug("Waiting for startOthers toggle")
            // wait for the first worker to process messages before also processing
            Await.result(startOthers, 1.minute)
            log.debug("Continuing after wait for startOthers toggle")
          }
        }
        sender() ! id
    }
  }

  class Parent extends Actor {
    val pool =
      context.actorOf(
        BalancingPool(2).props(
          routeeProps = Props(classOf[Worker], TestLatch(0)(context.system), Future.successful(()))))

    def receive = {
      case msg => pool.forward(msg)
    }
  }
}

class BalancingSpec extends AkkaSpec("""
    akka.loglevel=debug
    akka.actor.deployment {
      /balancingPool-2 {
        router = balancing-pool
        nr-of-instances = 5
        pool-dispatcher {
          attempt-teamwork = on
        }
      }
      /balancingPool-3 {
        router = balancing-pool
        nr-of-instances = 5
        pool-dispatcher {
          attempt-teamwork = on
        }
      }
    }
    """) with ImplicitSender with BeforeAndAfterEach {
  import BalancingSpec._

  val poolSize = 5 // must be less than fork-join parallelism-min, which is 8 in AkkaSpec

  override def beforeEach(): Unit = {
    counter.set(1)
  }

  def test(pool: ActorRef, startOthers: Promise[Unit], latch: TestLatch): Unit = {
    val probe = TestProbe()
    try {
      val iterationCount = 100

      for (i <- 1 to iterationCount) {
        pool.tell(i, probe.ref)
      }

      // all but one worker are blocked
      val replies1 = probe.receiveN(iterationCount - poolSize + 1)
      // all replies from the unblocked worker so far
      replies1.toSet should be(Set(1))
      log.debug("worker one: [{}]", probe.lastSender)
      probe.expectNoMessage(1.second)

      // Now unblock the other workers from also making progress
      startOthers.success(())
      val replies2 = probe.receiveN(poolSize - 1)
      // the remaining replies come from the blocked
      replies2.toSet should be((2 to poolSize).toSet)
      probe.expectNoMessage(500.millis)
    } finally {
      val watchProbe = TestProbe()
      // careful cleanup since threads may be blocked
      watchProbe.watch(pool)
      // make sure the latch and promise are not blocking actor threads
      startOthers.trySuccess(())
      latch.open()
      pool ! PoisonPill
      watchProbe.expectTerminated(pool)
    }
  }

  "balancing pool" must {

    // FIXME flaky, https://github.com/akka/akka-core/issues/30860
    pending

    "deliver messages in a balancing fashion when defined programatically" in {
      val latch = TestLatch(poolSize)
      val startOthers = Promise[Unit]()
      val pool = system.actorOf(
        BalancingPool(poolSize).props(routeeProps = Props(classOf[Worker], latch, startOthers.future)),
        name = "balancingPool-1")
      test(pool, startOthers, latch)
    }

    "deliver messages in a balancing fashion when defined in config" in {
      val latch = TestLatch(poolSize)
      val startOthers = Promise[Unit]()
      val pool =
        system.actorOf(
          FromConfig().props(routeeProps = Props(classOf[Worker], latch, startOthers.future)),
          name = "balancingPool-2")
      test(pool, startOthers, latch)
    }

    "deliver messages in a balancing fashion when overridden in config" in {
      val latch = TestLatch(poolSize)
      val startOthers = Promise[Unit]()
      val pool =
        system.actorOf(
          BalancingPool(1).props(routeeProps = Props(classOf[Worker], latch, startOthers.future)),
          name = "balancingPool-3")
      test(pool, startOthers, latch)
    }

    "work with anonymous actor names" in {
      // the dispatcher-id must not contain invalid config key characters (e.g. $a)
      system.actorOf(Props[Parent]()) ! 1000
      expectMsgType[Int]
    }

    "work with encoded actor names" in {
      val encName = URLEncoder.encode("abcå6#$€xyz", "utf-8")
      // % is a valid config key character (e.g. %C3%A5)
      system.actorOf(Props[Parent](), encName) ! 1001
      expectMsgType[Int]
    }

  }
}
