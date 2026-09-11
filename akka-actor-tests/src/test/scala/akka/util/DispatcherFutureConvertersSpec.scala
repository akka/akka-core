/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.testkit.AkkaSpec
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException

class DispatcherFutureConvertersSpec extends AkkaSpec {
  import DispatcherFutureConverters._

  def threadComponents(): Array[String] = {
    Thread.currentThread().getName().split('-')
  }

  "A Scala future should be convertible into a Java future which" must {
    val defaultDispatcherPrefix = Await.result(Future {
      threadComponents().dropRight(1)
    }(system.dispatcher), 3.seconds)

    def assertRunningOnDefaultDispatcher(): Assertion = {
      threadComponents().take(defaultDispatcherPrefix.size).zipWithIndex.foldLeft(succeed) { (_, pair) =>
        val (component, i) = pair
        component shouldBe defaultDispatcherPrefix(i)
      }
    }

    // Validate that the test is not itself running on the dispatcher
    a[TestFailedException] shouldBe thrownBy(assertRunningOnDefaultDispatcher())

    "propagate success" in {
      val promise = Promise[String]()
      val cs = asJava(promise.future, system.dispatcher)
      val cf = cs.toCompletableFuture()

      assert(!cf.isDone)
      promise.success("Hello")
      assert(cf.isDone)
      cf.get() shouldBe "Hello"
    }

    "propagate failure" in {
      val promise = Promise[String]()
      val cs = asJava(promise.future, system.dispatcher)
      val cf = cs.toCompletableFuture()
      val ex = new RuntimeException("Boom")

      promise.failure(ex)
      assert(cf.isDone)
      val wrapped = the[CompletionException] thrownBy (cf.join())
      wrapped.getCause shouldBe ex
    }

    "support thenApply (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).thenApply(_ => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        Done
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe Done
    }

    "support thenAccept (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).thenAccept(_ => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe null
    }

    "support thenRun (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).thenRun(() => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe null
    }

    "support thenCombine (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val other = CompletableFuture.completedStage[Integer](42)
      val cs = asJava(promise.future, system.dispatcher).thenCombine(other, (x, y: Integer) => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        s"$y $x"
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe "42 Hello"
    }

    "support thenAcceptBoth (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val other = CompletableFuture.completedStage[Integer](42)
      val cs = asJava(promise.future, system.dispatcher).thenAcceptBoth[Integer](other, (_, _) => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe null
    }

    "support runAfterBoth (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val other = CompletableFuture.completedStage[Integer](42)
      val cs = asJava(promise.future, system.dispatcher).runAfterBoth(other, () => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe null
    }

    "support applyToEither (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val other = new CompletableFuture[String]()
      val cs = asJava(promise.future, system.dispatcher).applyToEither(other, x => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        x.length
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe 5
    }

    "support acceptEither (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val other = new CompletableFuture[String]()
      val cs = asJava(promise.future, system.dispatcher).applyToEither(other, x => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        x.length
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe 5
    }

    "support runAfterEither (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val other = new CompletableFuture[String]()
      val cs = asJava(promise.future, system.dispatcher).runAfterEither(other, () => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe null
    }

    "support thenCompose (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).thenCompose(x => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        CompletableFuture.completedStage(s"$x 42")
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe "Hello 42"
    }

    "support whenComplete (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).whenComplete((_, _) => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe "Hello"
    }

    "support handle (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).handle((v, _) => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        v.length
      })
      val cf = cs.toCompletableFuture()

      promise.success("Hello")
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe 5
    }

    "support exceptionally (on the provided dispatcher)" in {
      val promise = Promise[String]()
      val latch = new CountDownLatch(1)
      val cs = asJava(promise.future, system.dispatcher).exceptionally(e => {
        assertRunningOnDefaultDispatcher()
        latch.await(1, SECONDS)
        e.getMessage
      })
      val cf = cs.toCompletableFuture()

      promise.failure(new RuntimeException("Goodbye"))
      assert(!cf.isDone)
      latch.countDown()
      cf.get() shouldBe "Goodbye"
    }

    "not allow completing the underlying promise from Java API" in {
      val promise = Promise[String]()
      val cf = asJava(promise.future, system.dispatcher).toCompletableFuture()

      cf.getNow("not yet") shouldBe "not yet"
      cf.complete("Java done")
      cf.get() shouldBe "Java done"
      assert(!promise.isCompleted)
    }

    "retain Scala completion when completing from Java after Scala" in {
      val promise = Promise[String]()
      val cf = asJava(promise.future, system.dispatcher).toCompletableFuture()

      promise.success("Scala done")
      cf.get() shouldBe "Scala done"
      cf.complete("Java done")
      cf.get() shouldBe "Scala done"
    }

    "retain Java completion when completing from Scala after Java" in {
      val promise = Promise[String]()
      val cf = asJava(promise.future, system.dispatcher).toCompletableFuture()

      cf.complete("Java done")
      cf.get() shouldBe "Java done"
      promise.success("Scala done")
      cf.get() shouldBe "Java done"
    }
  }
}
