/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util
import java.util.concurrent._
import java.util.concurrent.locks.{ Condition, LockSupport, ReentrantLock }

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.{ Await, ExecutionContext, ExecutionContextExecutor, Future }
import scala.util.control.Exception

import org.scalactic.source.Position
import org.scalatest.concurrent.{ Signaler, ThreadSignaler }
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.matchers.{ MatchResult, Matcher }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.wordspec.AnyWordSpec

import akka.testkit.TimingTest
import akka.util.DefaultExecutionContext._
import scala.jdk.CollectionConverters._

class BoundedBlockingQueueSpec
    extends AnyWordSpec
    with Matchers
    with QueueSetupHelper
    with CustomContainsMatcher
    with BlockingHelpers {

  import QueueTestEvents._

  "Constructor" must {
    "not accept a null backing Queue" in {
      intercept[IllegalArgumentException] {
        new BoundedBlockingQueue(10, null)
      }
    }

    "not accept a non-empty backing Queue" in {
      intercept[IllegalArgumentException] {
        val listQ = new util.LinkedList[String]()
        listQ.add("Hello")
        new BoundedBlockingQueue[String](10, listQ)
      }
    }

    "accept an empty backing Queue" in {
      new BoundedBlockingQueue(10, new util.LinkedList()).size() should equal(0)
    }

    "get a positive maxCapacity" in {
      intercept[IllegalArgumentException] {
        new BoundedBlockingQueue(0, new util.LinkedList())
      }
    }

    "not accept a backing queue with lower capacity" in {
      intercept[IllegalArgumentException] {
        new BoundedBlockingQueue(2, new LinkedBlockingQueue(1))
      }
    }

  }

  "remainingCapacity" must {
    "check the backing queue size" in {
      val TestContext(queue, events, _, _, _, backingQueue) = newBoundedBlockingQueue(1)

      backingQueue.offer("1")
      queue.remainingCapacity() shouldBe 0
      events should contain(getSize)
    }
  }

  "put" must {
    "reject null elements" in {
      val queue = newBoundedBlockingQueue(1).queue
      intercept[NullPointerException] {
        queue.put(null)
      }
    }

    "call the backing queue if not full" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.put("World")
      (events should contain).inOrder(offer("Hello"), offer("World"))
    }

    "signal notEmpty when an element is inserted" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")
      events should contain(signalNotEmpty)
    }

    "block when the queue is full" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.offer("1")

      mustBlockFor(100.milliseconds) {
        queue.put("2")
      }
      (events should contain).inOrder(offer("1"), awaitNotFull)
      events should not contain offer("2")
    }

    "block until the backing queue has space" taggedAs TimingTest in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.offer("a")

      val latch = new CountDownLatch(1)
      val f = Future {
        latch.countDown()
        queue.put("b")
      }

      latch.await(3, TimeUnit.SECONDS)
      // queue.take() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      queue.take()

      Await.result(f, 3.seconds)
      (events should contain).inOrder(offer("a"), poll, offer("b"))
    }

    "check the backing queue size before offering" taggedAs TimingTest in {
      val TestContext(queue, events, _, notFull, lock, _) = newBoundedBlockingQueue(1)
      queue.offer("a")

      val latch = new CountDownLatch(1)
      // Blocks until another thread signals `notFull`
      val f = Future {
        latch.countDown()
        queue.put("b")
      }

      latch.await(3, TimeUnit.SECONDS)
      // queue.put() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      lock.lockInterruptibly()
      notFull.signal()
      lock.unlock()

      mustBlockFor(100.milliseconds, f)
      events.toList should containInSequence(offer("a"), awaitNotFull, signalNotFull, getSize, awaitNotFull)
      events shouldNot contain(offer("b"))
    }
  }

  "take" must {
    "call the backing queue if not empty" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")
      queue.take()

      (events should contain).inOrder(offer("Hello"), poll)
    }

    "signal notFull when taking an element" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")
      queue.take()
      events should contain(signalNotFull)
    }

    "block when the queue is empty" taggedAs TimingTest in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)

      val latch = new CountDownLatch(1)
      mustBlockFor(100.milliseconds) {
        latch.countDown()
        queue.take()
      }

      latch.await(3, TimeUnit.SECONDS)
      // queue.take() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      events should contain(awaitNotEmpty)
      events should not contain (poll)
    }

    "block until the backing queue is non-empty" taggedAs TimingTest in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)

      val latch = new CountDownLatch(1)
      val f = Future {
        latch.countDown()
        queue.take()
      }

      latch.await(3, TimeUnit.SECONDS)
      // queue.take() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      queue.put("a")

      Await.ready(f, 3.seconds)
      (events should contain).inOrder(awaitNotEmpty, offer("a"), poll)
    }

    "check the backing queue size before polling" taggedAs TimingTest in {
      val TestContext(queue, events, notEmpty, _, lock, _) = newBoundedBlockingQueue(1)

      val latch = new CountDownLatch(1)
      // Blocks until another thread signals `notEmpty`
      val f = Future {
        latch.countDown()
        queue.take()
      }

      // Cause `notFull` signal, but don't fill the queue
      latch.await(3, TimeUnit.SECONDS)
      // queue.take() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      lock.lockInterruptibly()
      notEmpty.signal()
      lock.unlock()

      // `f` should still block since the queue is still empty
      mustBlockFor(100.milliseconds, f)
      events.toList should containInSequence(getSize, awaitNotEmpty, signalNotEmpty, getSize, awaitNotEmpty)
      events shouldNot contain(poll)
    }
  }

  "offer" must {
    "reject null elements" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      intercept[NullPointerException] {
        queue.offer(null)
      }
    }

    "not block if the queue is full" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.offer("Hello") should equal(true)
      queue.offer("World") should equal(false)
      events should not contain offer("World")
    }

    "call the backing queue if not full" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.offer("Hello") should equal(true)
      queue.offer("World") should equal(true)
      (events should contain).inOrder(offer("Hello"), offer("World"))
    }

    "signal notEmpty when the call succeeds" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.offer("Hello")
      events should contain(signalNotEmpty)
    }
  }

  "offer with timeout" must {
    "not block if the queue is not full" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.offer("Hello", 100, TimeUnit.MILLISECONDS) should equal(true)
      events should contain(offer("Hello"))
    }

    "signal notEmpty if the queue is not full" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.offer("Hello", 100, TimeUnit.MILLISECONDS) should equal(true)
      events should contain(signalNotEmpty)
    }

    "block for at least the timeout if the queue is full" taggedAs TimingTest in {
      val TestContext(queue, events, _, notFull, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")

      notFull.manualTimeControl(true)

      val latch = new CountDownLatch(1)
      val f = Future {
        latch.countDown()
        queue.offer("World", 100, TimeUnit.MILLISECONDS)
      }

      latch.await(3, TimeUnit.SECONDS)
      // queue.offer() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      notFull.advanceTime(99.milliseconds)

      mustBlockFor(100.milliseconds, f)
      events shouldNot contain(offer("World"))
    }

    "return false if the timeout is exceeded" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")
      queue.offer("World", 100, TimeUnit.MILLISECONDS) should equal(false)
      events shouldNot contain(offer("World"))
    }

    "block for less than the timeout when the queue becomes not full" taggedAs TimingTest in {

      val TestContext(queue, events, _, notFull, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")

      notFull.manualTimeControl(true)

      val latch = new CountDownLatch(1)
      val f = Future {
        latch.countDown()
        queue.offer("World", 100, TimeUnit.MILLISECONDS)
      }
      notFull.advanceTime(99.milliseconds)

      latch.await(3, TimeUnit.SECONDS)
      // queue.offer() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      queue.take()

      Await.result(f, 3.seconds) should equal(true)
      (events should contain).inOrder(awaitNotFull, signalNotFull, offer("World"))
    }

    "check the backing queue size before offering" taggedAs TimingTest in {
      val TestContext(queue, events, _, notFull, lock, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")

      val latch = new CountDownLatch(1)
      // Blocks until another thread signals `notFull`
      val f = Future {
        latch.countDown()
        queue.offer("World", 1000, TimeUnit.DAYS)
      }

      // Cause `notFull` signal, but don't fill the queue
      latch.await(3, TimeUnit.SECONDS)
      // queue.offer() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      lock.lockInterruptibly()
      notFull.signal()
      lock.unlock()

      // `f` should still block since the queue is still empty
      mustBlockFor(100.milliseconds, f)
      events.toList should containInSequence(getSize, awaitNotFull, signalNotFull, getSize, awaitNotFull)
      events shouldNot contain(offer("World"))
    }
  }

  "poll" must {
    "return null when the queue is empty" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.poll() should equal(null)
    }

    "poll the backing queue" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.poll()
      events should contain(poll)
    }

    "return the first element inserted" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.put("World")
      queue.poll() should equal("Hello")
    }

    "signal notEmpty when an element is removed" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")
      queue.poll()
      events should contain(signalNotEmpty)
    }
  }

  "poll with timeout" must {
    "not block if the queue is not empty" in {
      val TestContext(queue, events, _, _, _, backingQueue) = newBoundedBlockingQueue(1)
      backingQueue.offer("Hello")
      queue.poll(1, TimeUnit.DAYS) should equal("Hello")
      events should contain(poll)
    }

    "signal notFull when an element is removed" in {
      val TestContext(queue, events, _, _, _, backingQueue) = newBoundedBlockingQueue(1)
      backingQueue.offer("Hello")

      queue.poll(1, TimeUnit.DAYS)
      events should contain(signalNotFull)
    }

    "block for at least the timeout if the queue is empty" taggedAs TimingTest in {
      val TestContext(queue, events, notEmpty, _, _, _) = newBoundedBlockingQueue(1)
      notEmpty.manualTimeControl(true)

      val latch = new CountDownLatch(1)
      val f = Future {
        latch.countDown()
        queue.poll(100, TimeUnit.MILLISECONDS)
      }

      latch.await(3, TimeUnit.SECONDS)
      // queue.poll() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      notEmpty.advanceTime(99.milliseconds)

      mustBlockFor(100.milliseconds, f)
      events should contain(awaitNotEmpty)
    }

    "immediately poll the backing queue" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)

      queue.poll(100, TimeUnit.MILLISECONDS)
      events should contain(poll)
    }

    "return null if the timeout is exceeded" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)

      queue.poll(100, TimeUnit.MILLISECONDS) should equal(null)
    }

    "block for less than the timeout when the queue becomes non-empty" taggedAs TimingTest in {
      val TestContext(queue, events, notEmpty, _, _, _) = newBoundedBlockingQueue(1)

      notEmpty.manualTimeControl(true)

      val latch = new CountDownLatch(1)
      val f = Future {
        latch.countDown()
        queue.poll(100, TimeUnit.MILLISECONDS)
      }

      notEmpty.advanceTime(99.milliseconds)
      latch.await(3, TimeUnit.SECONDS)
      // queue.poll() must happen first
      Thread.sleep(50) // this is why this test is tagged as TimingTest
      f.isCompleted should be(false)
      queue.put("Hello")

      Await.result(f, 3.seconds) should equal("Hello")
      (events should contain).inOrder(awaitNotEmpty, signalNotEmpty, poll)
    }
  }

  "remove" must {
    "return false when the queue is empty" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.remove("Hello") should equal(false)
    }

    "return false when the queue does not contain the element" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("H3llo")
      queue.remove("Hello") should equal(false)
    }

    "return true if the element was removed" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.remove("Hello") should equal(true)
    }

    "signal notFull when an element is removed" in {
      val TestContext(queue, events, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.put("Hello")
      queue.remove("Hello")
      events should contain(signalNotFull)
    }
  }

  "contains" must {
    "return false when the queue is empty" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.contains("Hello") should equal(false)
    }

    "return false when the queue does not contain the element" in {
      val TestContext(queue, _, _, _, _, backingQueue) = newBoundedBlockingQueue(1)
      backingQueue.offer("H3llo")
      queue.contains("Hello") should equal(false)
    }

    "return true if the backing queue contains the element" in {
      val TestContext(queue, _, _, _, _, backingQueue) = newBoundedBlockingQueue(2)
      backingQueue.offer("Hello")
      queue.contains("Hello") should equal(true)
    }
  }

  "clear" must {
    "remove all elements from the backing queue" in {
      val TestContext(queue, _, _, _, _, backingQueue) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.clear()
      backingQueue.size() should equal(0)
    }
  }

  "peek" must {
    "return the first element in the queue" in {
      val TestContext(queue, _, _, _, _, backingQueue) = newBoundedBlockingQueue(2)
      backingQueue.offer("Hello")
      queue.peek() should equal("Hello")
    }

    "not remove from the queue" in {
      val TestContext(queue, _, _, _, _, backingQueue) = newBoundedBlockingQueue(1)
      backingQueue.offer("Hello")
      queue.peek()
      backingQueue.contains("Hello") should equal(true)
    }
  }

  "drainTo" must {
    "not accept a null argument" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      intercept[NullPointerException] {
        queue.drainTo(null)
      }
    }

    "not accept the source queue as argument" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      intercept[IllegalArgumentException] {
        queue.drainTo(queue)
      }
    }

    "not accept the backing queue as argument" in {
      val TestContext(queue, _, _, _, _, backingQueue) = newBoundedBlockingQueue(1)
      intercept[IllegalArgumentException] {
        queue.drainTo(backingQueue)
      }
    }

    "remove all elements from the queue" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.put("World")
      queue.drainTo(new util.LinkedList[String]())
      queue.remainingCapacity() should equal(2)
    }

    "add all elements to the target collection" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      val elems = List("Hello", "World")
      val target = mutable.Buffer[String]()
      elems.foreach(queue.put)
      queue.drainTo(target.asJava)
      elems should contain theSameElementsAs (target)
    }
  }

  "containsAll" must {
    "return false if the queue contains a subset" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      val elems = List("Hello", "World")
      elems.foreach(queue.put)
      queue.containsAll(("Akka" :: elems).asJava) should equal(false)
    }

    "returns true if the queue contains exactly all elements" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      val elems = List("Hello", "World")
      elems.foreach(queue.put)
      queue.containsAll(elems.asJava) should equal(true)
    }

    "returns true if the queue contains more elements" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(3)
      val elems = List("Hello", "World")
      elems.foreach(queue.put)
      queue.put("Akka")
      queue.containsAll(elems.asJava) should equal(true)
    }
  }

  "removeAll" must {
    "remove all elements from the argument collection" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(3)
      val elems = List("Hello", "World")
      elems.foreach(queue.put)
      queue.put("Akka")

      queue.removeAll(elems.asJava) should equal(true)
      queue.remainingCapacity() should equal(2)
      queue.poll() should equal("Akka")
    }

    "return false if no elements were removed" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      val elems = List("Hello", "World")
      queue.put("Akka")

      queue.removeAll(elems.asJava) should equal(false)
      queue.poll() should equal("Akka")
    }
  }

  "retainAll" must {
    "remove all elements not in the argument collection" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(3)
      val elems = List("Hello", "World")
      elems.foreach(queue.put)
      queue.put("Akka")

      queue.retainAll(elems.asJava) should equal(true)
      queue.remainingCapacity() should equal(1)
      queue.toArray() shouldNot contain("Akka")
      queue.toArray() should contain theSameElementsAs (elems)
    }

    "return false if no elements were removed" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      val elems = List("Hello", "World", "Akka")
      queue.put("Hello")
      queue.put("World")

      queue.retainAll(elems.asJava) should equal(false)
      (queue.toArray() should contain).allOf("Hello", "World")
    }
  }

  "iterator" must {
    "not any elements if the queue is empty" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(1)
      queue.iterator().hasNext() should equal(false)
    }

    "disallow remove() before calling next()" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.put("Akka")

      val iter = queue.iterator()
      intercept[IllegalStateException] {
        iter.remove()
      }
    }

    "disallow two subsequent remove()s" in {
      val TestContext(queue, _, _, _, _, _) = newBoundedBlockingQueue(2)
      queue.put("Hello")
      queue.put("Akka")

      val iter = queue.iterator()
      iter.next()
      iter.remove()
      intercept[IllegalStateException] {
        iter.remove()
      }
    }
  }

}

/**
 * Custom trait for matching sequences.
 *
 * The following will succeed:
 *   `List(0,1,2,3,1) should containInSequence(1,2,3)`
 *
 * Note that this fails with `... should contain inOrder ...`
 */
trait CustomContainsMatcher {

  class ContainsInSequenceMatcher[A](right: List[A]) extends Matcher[Seq[A]] {

    def apply(left: Seq[A]): MatchResult = {

      def attemptMatch(remainingTruth: List[A], remainingSequence: List[A]): MatchResult =
        (remainingTruth, remainingSequence) match {
          case (_, Nil)                     => matchResult(true)
          case (Nil, _)                     => matchResult(false)
          case (x :: xs, y :: ys) if x == y => attemptMatch(xs, ys)
          case (_ :: xs, ys)                => attemptMatch(xs, ys)
        }

      def matchResult(success: Boolean): MatchResult =
        MatchResult(
          success,
          s"""$left did not contain all of $right in sequence""",
          s"""$left contains all of $right in sequence""")

      attemptMatch(left.toList, right)
    }
  }
  def containInSequence(one: Any, two: Any, rest: Any*) = new ContainsInSequenceMatcher[Any](one :: two :: rest.toList)
}

/**
 * Some helper methods for dealing with things that should be blocking.
 * Whether or not a call blocks is impossible to determine precisely,
 * therefore we resort to checking that a call does not return within a set time.
 */
trait BlockingHelpers {
  import org.scalatest.Assertions._
  import org.scalatest.concurrent.TimeLimits._

  implicit val timeoutSignaler: Signaler = ThreadSignaler

  /**
   * Delay execution of some code until the timespan has passed.
   * The action is wrapped in a Future that initially calls `Thread.sleep()`
   */
  def after(timespan: Span)(action: => Unit): Future[Unit] = Future {
    LockSupport.parkNanos(timespan.toNanos)
    action
  }

  /**
   * Check that a call does not return within a set timespan.
   */
  def mustBlockFor(timeout: Span)(action: => Unit)(implicit pos: Position): Unit =
    Exception.ignoring(classOf[TestFailedDueToTimeoutException]) {
      failAfter(timeout)(action)
      fail("Expected action to block for at least " + timeout.prettyString + " but it completed.")
    }

  /**
   * Check that a Future does not complete within a set timespan.
   */
  def mustBlockFor(timeout: Span, action: Future[_])(implicit pos: Position): Unit =
    Exception.ignoring(classOf[TimeoutException]) {
      Await.ready(action, timeout)
      fail("Expected action to block for at least " + timeout.prettyString + " but it completed.")
    }

}

/**
 * All events that can be recorded and asserted during a test.
 */
object QueueTestEvents {
  sealed abstract class QueueEvent
  case class Poll() extends QueueEvent
  case class Offer(value: String) extends QueueEvent
  case class GetSize() extends QueueEvent
  case class AwaitNotEmpty() extends QueueEvent
  case class AwaitNotFull() extends QueueEvent
  case class SignalNotEmpty() extends QueueEvent
  case class SignalNotFull() extends QueueEvent

  val poll = Poll()
  val getSize = GetSize()
  def offer(value: String) = Offer(value)
  val signalNotEmpty = SignalNotEmpty()
  val signalNotFull = SignalNotFull()
  val awaitNotEmpty = AwaitNotEmpty()
  val awaitNotFull = AwaitNotFull()
}

/**
 * Helper for setting up a queue under test with injected lock, conditions and backing queue.
 */
trait QueueSetupHelper {
  import java.util.Date

  import akka.util.QueueTestEvents._

  case class TestContext(
      queue: BoundedBlockingQueue[String],
      events: mutable.Buffer[QueueEvent],
      notEmpty: TestCondition,
      notFull: TestCondition,
      lock: ReentrantLock,
      backingQueue: util.Queue[String])

  /**
   * Backing queue that records all poll and offer calls in `events`
   */
  class TestBackingQueue(events: mutable.Buffer[QueueEvent]) extends util.LinkedList[String] {

    override def poll(): String = {
      events += Poll()
      super.poll()
    }

    override def offer(e: String): Boolean = {
      events += Offer(e)
      super.offer(e)
    }

    override def size(): Int = {
      events += GetSize()
      super.size()
    }
  }

  /**
   * Reentrant lock condition that records when the condition is signaled or `await`ed.
   */
  class TestCondition(
      events: mutable.Buffer[QueueEvent],
      condition: Condition,
      signalEvent: QueueEvent,
      awaitEvent: QueueEvent)
      extends Condition {

    case class Manual(waitTime: Long = 0, waitingThread: Option[Thread] = None)

    @volatile private var waiting: Option[Manual] = None

    def advanceTime(timespan: Span): Unit = {
      waiting match {
        case Some(manual) =>
          val newWaitTime = manual.waitTime - timespan.toNanos
          waiting = if (newWaitTime <= 0 && manual.waitingThread.isDefined) {
            manual.waitingThread.get.interrupt()
            Some(Manual(newWaitTime, None))
          } else {
            Some(manual.copy(waitTime = newWaitTime))
          }

        case None =>
          sys.error("Called advance time but hasn't enabled manualTimeControl")
      }
    }

    def manualTimeControl(@nowarn("msg=never used") on: Boolean): Unit =
      waiting = Some(Manual())

    override def signalAll(): Unit = condition.signalAll()

    override def awaitNanos(nanosTimeout: Long): Long = {
      if (waiting.isEmpty) {
        events += awaitEvent
        condition.awaitNanos(nanosTimeout)
      } else {
        val waitTime = nanosTimeout
        val waitingThread = Some(Thread.currentThread())
        waiting = Some(Manual(waitTime, waitingThread))

        try {
          this.await()
        } catch {
          case _: InterruptedException =>
        }
        waitTime
      }
    }

    override def awaitUninterruptibly(): Unit = condition.awaitUninterruptibly()

    override def await(): Unit = {
      events += awaitEvent
      condition.await()
    }

    override def await(time: Long, unit: TimeUnit): Boolean = condition.await(time, unit)

    override def signal(): Unit = {
      events += signalEvent
      condition.signal()
    }

    override def awaitUntil(deadline: Date): Boolean = condition.awaitUntil(deadline)
  }

  def newBoundedBlockingQueue(maxCapacity: Int): TestContext = {
    val events: mutable.Buffer[QueueEvent] = new mutable.ArrayBuffer()

    val realLock = new ReentrantLock(false)
    val wrappedNotEmpty = new TestCondition(events, realLock.newCondition(), signalNotEmpty, awaitNotEmpty)
    val wrappedNotFull = new TestCondition(events, realLock.newCondition(), signalNotFull, awaitNotFull)

    val backingQueue = new TestBackingQueue(events)

    /**
     * Class under test with the necessary backing queue, lock and conditions injected.
     */
    class TestBoundedBlockingQueue() extends BoundedBlockingQueue[String](maxCapacity, backingQueue) {

      override def createLock(): ReentrantLock = realLock
      override def createNotEmptyCondition(): Condition = wrappedNotEmpty
      override def createNotFullCondition(): Condition = wrappedNotFull
    }

    TestContext(new TestBoundedBlockingQueue(), events, wrappedNotEmpty, wrappedNotFull, realLock, backingQueue)
  }
}

/**
 * Schedule every Future on a new thread.
 * Ensures that we don't accidentally run things on one thread that should be concurrent.
 */
object DefaultExecutionContext {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(new Executor {
    override def execute(command: Runnable): Unit =
      new Thread() {
        override def run(): Unit = command.run()
      }.start()
  })
}
