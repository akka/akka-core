/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.time.LocalDateTime

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Random, Try }

import MetricsBasedResizerSpec._
import OptimalSizeExploringResizer._

import akka.actor._
import akka.pattern.ask
import akka.testkit._
import akka.testkit.TestEvent._
import akka.util.Timeout

object MetricsBasedResizerSpec {

  case class Latches(first: TestLatch, second: TestLatch)

  /**
   * The point of these Actors is that their mailbox size will be queried
   * by the resizer.
   */
  class TestLatchingActor(implicit timeout: Timeout) extends Actor {

    def receive = {
      case Latches(first, second) =>
        first.countDown()
        Try(Await.ready(second, timeout.duration))
    }
  }

  def routee(implicit system: ActorSystem, timeout: Timeout): ActorRefRoutee =
    ActorRefRoutee(system.actorOf(Props(new TestLatchingActor)))

  def routees(num: Int = 10)(implicit system: ActorSystem, timeout: Timeout) = (1 to num).map(_ => routee).toVector

  case class TestRouter(routees: Vector[ActorRefRoutee])(implicit system: ActorSystem, timeout: Timeout) {

    var msgs: Set[TestLatch] = Set()

    def mockSend(
        await: Boolean,
        l: TestLatch = TestLatch(),
        routeeIdx: Int = Random.nextInt(routees.length)): Latches = {
      val target = routees(routeeIdx)
      val first = TestLatch()
      val latches = Latches(first, l)
      target.send(latches, Actor.noSender)
      msgs = msgs + l
      if (await) Await.ready(first, timeout.duration)
      latches
    }

    def close(): Unit = msgs.foreach(_.open())

    def sendToAll(await: Boolean): Seq[Latches] = {
      val sentMessages = routees.indices.map(i => mockSend(await, routeeIdx = i))
      sentMessages
    }

  }

}

class MetricsBasedResizerSpec extends AkkaSpec(ResizerSpec.config) with DefaultTimeout with ImplicitSender {

  override def atStartup(): Unit = {
    // when shutting down some Resize messages might hang around
    system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*Resize")))
  }

  "MetricsBasedResizer isTimeForResize" must {

    "be true with empty history" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.record = ResizeRecord(checkTime = 0)
      resizer.isTimeForResize(0) should ===(true)
    }

    "be false if the last resize is too close within actionInterval enough history" in {
      val resizer = DefaultOptimalSizeExploringResizer(actionInterval = 10.seconds)
      resizer.record = ResizeRecord(checkTime = System.nanoTime() - 8.seconds.toNanos)

      resizer.isTimeForResize(100) should ===(false)
    }

    "be true if the last resize is before actionInterval ago" in {
      val resizer = DefaultOptimalSizeExploringResizer(actionInterval = 10.seconds)
      resizer.record = ResizeRecord(checkTime = System.nanoTime() - 11.seconds.toNanos)

      resizer.isTimeForResize(100) should ===(true)
    }

  }

  "MetricsBasedResizer reportMessageCount" must {

    "record last messageCounter correctly" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.reportMessageCount(Vector(routee), 3)
      resizer.record.messageCount shouldBe 3
    }

    "record last totalQueueLength correctly" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      val router = TestRouter(routees(2))

      resizer.reportMessageCount(router.routees, router.msgs.size)
      resizer.record.totalQueueLength shouldBe 0

      router.sendToAll(await = true)
      router.mockSend(await = false) // test one message in mailbox and one in each ActorCell

      resizer.reportMessageCount(router.routees, router.msgs.size)
      resizer.record.totalQueueLength shouldBe 3

      router.close()
    }

    "start an underutilizationStreak when not fully utilized" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.reportMessageCount(routees(2), 0)
      resizer.record.underutilizationStreak should not be empty
      resizer.record.underutilizationStreak.get.start.isBefore(LocalDateTime.now.plusSeconds(1)) shouldBe true
      resizer.record.underutilizationStreak.get.start.isAfter(LocalDateTime.now.minusSeconds(1)) shouldBe true
    }

    "stop an underutilizationStreak when fully utilized" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.record = ResizeRecord(
        underutilizationStreak =
          Some(UnderUtilizationStreak(start = LocalDateTime.now.minusHours(1), highestUtilization = 1)))

      val router = TestRouter(routees(2))
      router.sendToAll(await = true)

      resizer.reportMessageCount(router.routees, router.msgs.size)
      resizer.record.underutilizationStreak shouldBe empty

      router.close()
    }

    "leave the underutilizationStreak start date unchanged when not fully utilized" in {
      val start: LocalDateTime = LocalDateTime.now.minusHours(1)
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.record =
        ResizeRecord(underutilizationStreak = Some(UnderUtilizationStreak(start = start, highestUtilization = 1)))

      resizer.reportMessageCount(routees(2), 0)
      resizer.record.underutilizationStreak.get.start shouldBe start
    }

    "leave the underutilizationStreak highestUtilization unchanged if current utilization is lower" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.record = ResizeRecord(
        underutilizationStreak = Some(UnderUtilizationStreak(start = LocalDateTime.now, highestUtilization = 2)))

      val router = TestRouter(routees(2))
      router.mockSend(await = true)

      resizer.reportMessageCount(router.routees, router.msgs.size)
      resizer.record.underutilizationStreak.get.highestUtilization shouldBe 2

      router.close()
    }

    // disabled in CI since flaky https://github.com/akka/akka-core/issues/32401
    "update the underutilizationStreak highestUtilization if current utilization is higher" taggedAs (GHExcludeTest) in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.record = ResizeRecord(
        underutilizationStreak = Some(UnderUtilizationStreak(start = LocalDateTime.now, highestUtilization = 1)))

      val router = TestRouter(routees(3))
      router.mockSend(await = true, routeeIdx = 0)
      router.mockSend(await = true, routeeIdx = 1)

      resizer.reportMessageCount(router.routees, router.msgs.size)
      resizer.record.underutilizationStreak.get.highestUtilization shouldBe 2

      router.close()
    }

    "not record a performance log when it's not fully utilized in two consecutive checks" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      val router = TestRouter(routees(2))
      resizer.reportMessageCount(router.routees, router.msgs.size)

      router.sendToAll(await = true)
      resizer.reportMessageCount(router.routees, router.msgs.size)

      resizer.performanceLog shouldBe empty

      router.close()
    }

    "not record the performance log when no message is processed" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.record = ResizeRecord(totalQueueLength = 2, messageCount = 2, checkTime = System.nanoTime())

      val router = TestRouter(routees(2))

      router.sendToAll(await = true)
      resizer.reportMessageCount(router.routees, router.msgs.size)

      resizer.performanceLog shouldBe empty

      router.close()
    }

    // disabled in CI since flaky https://github.com/akka/akka-core/issues/32401
    "record the performance log with the correct pool size" taggedAs (GHExcludeTest) in {
      val resizer = DefaultOptimalSizeExploringResizer()
      val router = TestRouter(routees(2))
      val msgs = router.sendToAll(await = true)
      resizer.reportMessageCount(router.routees, router.msgs.size)
      msgs.head.second.open()

      router.mockSend(await = true, routeeIdx = 0)
      router.mockSend(await = false, routeeIdx = 1)
      resizer.reportMessageCount(router.routees, router.msgs.size)
      resizer.performanceLog.get(2) should not be empty

      router.close()
    }

    "record the performance log with the correct process speed" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      val router = TestRouter(routees(2))
      val msgs1 = router.sendToAll(await = true)
      val msgs2 = router.sendToAll(await = false) //make sure the routees are still busy after the first batch of messages get processed.

      val before = System.nanoTime()
      resizer.reportMessageCount(router.routees, router.msgs.size) //updates the records

      msgs1.foreach(_.second.open()) //process two messages

      // make sure some time passes in-between
      Thread.sleep(300)

      // wait for routees to update their mail boxes
      msgs2.foreach(l => Await.ready(l.first, timeout.duration))

      resizer.reportMessageCount(router.routees, router.msgs.size)

      val after = System.nanoTime()
      val millisPassed = (after - before) / 1000000
      val tenPercent = millisPassed / 10
      resizer.performanceLog(2).toMillis shouldBe (millisPassed / 2 +- tenPercent)

      router.close()
    }

    "update the old performance log entry with updated speed " in {
      val oldSpeed = 50
      val resizer = DefaultOptimalSizeExploringResizer(weightOfLatestMetric = 0.5)

      resizer.performanceLog = Map(2 -> oldSpeed.milliseconds)

      val router = TestRouter(routees(2))
      val msgs1 = router.sendToAll(await = true)
      val msgs2 = router.sendToAll(await = false) //make sure the routees are still busy after the first batch of messages get processed.

      val before = System.nanoTime()
      resizer.reportMessageCount(router.routees, router.msgs.size) //updates the records

      msgs1.foreach(_.second.open()) //process two messages

      // make sure some time passes in-between
      Thread.sleep(300)

      // wait for routees to update their mail boxes
      msgs2.foreach(l => Await.ready(l.first, timeout.duration))

      resizer.reportMessageCount(router.routees, router.msgs.size)

      val after = System.nanoTime()
      val millisPassed = (after - before) / 1000000
      val tenPercent = millisPassed / 10
      val newSpeed = millisPassed / 2

      resizer.performanceLog(2).toMillis shouldBe ((newSpeed + oldSpeed) / 2 +- tenPercent)

      router.close()
    }

  }

  "MetricsBasedResizer resize" must {
    "downsize to close to the highest retention when a streak of underutilization started downsizeAfterUnderutilizedFor" in {
      val resizer = DefaultOptimalSizeExploringResizer(downsizeAfterUnderutilizedFor = 72.hours, downsizeRatio = 0.5)

      resizer.record = ResizeRecord(
        underutilizationStreak =
          Some(UnderUtilizationStreak(start = LocalDateTime.now.minusHours(73), highestUtilization = 8)))
      resizer.resize(routees(20)) should be(4 - 20)
    }

    "does not downsize on empty history" in {
      val resizer = DefaultOptimalSizeExploringResizer()
      resizer.resize(routees()) should be(0)
    }

    "always go to lowerBound if below it" in {
      val resizer = DefaultOptimalSizeExploringResizer(lowerBound = 50, upperBound = 100)
      resizer.resize(routees(20)) should be(30)
    }

    "always go to uppperBound if above it" in {
      val resizer = DefaultOptimalSizeExploringResizer(upperBound = 50)
      resizer.resize(routees(80)) should be(-30)
    }

    "explore when there is performance log but not go beyond exploreStepSize" in {
      val resizer = DefaultOptimalSizeExploringResizer(exploreStepSize = 0.3, explorationProbability = 1)
      resizer.performanceLog = Map(11 -> 1.milli, 13 -> 1.millis, 12 -> 3.millis)

      val exploreSamples = (1 to 100).map(_ => resizer.resize(routees(10)))
      exploreSamples.forall(change => Math.abs(change) >= 1 && Math.abs(change) <= (10 * 0.3)) should be(true)

    }
  }

  "MetricsBasedResizer optimize" must {
    "optimize towards the fastest pool size" in {
      val resizer = DefaultOptimalSizeExploringResizer(explorationProbability = 0)
      resizer.performanceLog = Map(7 -> 5.millis, 10 -> 3.millis, 11 -> 2.millis, 12 -> 4.millis)
      resizer.resize(routees(10)) should be(1)
      resizer.resize(routees(12)) should be(-1)
      resizer.resize(routees(7)) should be(2)
    }

    "ignore further away sample data when optimizing" in {
      val resizer = DefaultOptimalSizeExploringResizer(
        explorationProbability = 0,
        numOfAdjacentSizesToConsiderDuringOptimization = 4)
      resizer.performanceLog =
        Map(7 -> 5.millis, 8 -> 2.millis, 10 -> 3.millis, 11 -> 4.millis, 12 -> 3.millis, 13 -> 1.millis)

      resizer.resize(routees(10)) should be(-1)
    }
  }

  "MetricsBasedResizer" must {

    def poolSize(router: ActorRef): Int =
      Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees.size

    "start with lowerbound pool size" in {

      val resizer = DefaultOptimalSizeExploringResizer(lowerBound = 2)
      val router =
        system.actorOf(RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)).props(Props(new TestLatchingActor)))
      val latches = Latches(TestLatch(), TestLatch(0))
      router ! latches
      Await.ready(latches.first, timeout.duration)

      poolSize(router) shouldBe resizer.lowerBound
    }

  }

}
