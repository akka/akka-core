/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.{ Executor, ExecutorService, Executors }
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, ExecutionContextExecutorService }
import scala.concurrent.{ blocking, Await, Future, Promise }
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.Props
import akka.testkit.{ AkkaSpec, DefaultTimeout, TestLatch }
import akka.testkit.CallingThreadDispatcher
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import akka.util.SerializedSuspendableExecutionContext

class ExecutionContextSpec extends AkkaSpec with DefaultTimeout {

  "An ExecutionContext" must {

    "be instantiable" in {
      val es = Executors.newCachedThreadPool()
      try {
        val executor: Executor with ExecutionContext = ExecutionContext.fromExecutor(es)
        executor should not be (null)

        val executorService: ExecutorService with ExecutionContext = ExecutionContext.fromExecutorService(es)
        executorService should not be (null)

        val jExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(es)
        jExecutor should not be (null)

        val jExecutorService: ExecutionContextExecutorService = ExecutionContexts.fromExecutorService(es)
        jExecutorService should not be (null)
      } finally {
        es.shutdown
      }
    }

    "be able to use Batching" in {
      system.dispatcher.isInstanceOf[BatchingExecutor] should ===(true)

      import system.dispatcher

      def batchable[T](f: => T)(implicit ec: ExecutionContext): Unit =
        ec.execute(new Batchable {
          override def isBatchable = true
          override def run: Unit = f
        })

      val p = Promise[Unit]()
      batchable {
        val lock, callingThreadLock, count = new AtomicInteger(0)
        callingThreadLock.compareAndSet(0, 1) // Enable the lock
        (1 to 100).foreach { _ =>
          batchable {
            if (callingThreadLock.get != 0) p.tryFailure(new IllegalStateException("Batch was executed inline!"))
            else if (count.incrementAndGet == 100) p.trySuccess(()) //Done
            else if (lock.compareAndSet(0, 1)) {
              try Thread.sleep(10)
              finally lock.compareAndSet(1, 0)
            } else p.tryFailure(new IllegalStateException("Executed batch in parallel!"))
          }
        }
        callingThreadLock.compareAndSet(1, 0) // Disable the lock
      }
      Await.result(p.future, timeout.duration) should ===(())
    }

    "be able to avoid starvation when Batching is used and Await/blocking is called" in {
      system.dispatcher.isInstanceOf[BatchingExecutor] should ===(true)
      import system.dispatcher

      def batchable[T](f: => T)(implicit ec: ExecutionContext): Unit =
        ec.execute(new Batchable {
          override def isBatchable = true
          override def run(): Unit = f
        })

      val latch = TestLatch(101)
      batchable {
        (1 to 100).foreach { _ =>
          batchable {
            val deadlock = TestLatch(1)
            batchable { deadlock.open() }
            Await.ready(deadlock, timeout.duration)
            latch.countDown()
          }
        }
        latch.countDown()
      }
      Await.ready(latch, timeout.duration)
    }

    "work with tasks that use blocking{} multiple times" in {
      system.dispatcher.isInstanceOf[BatchingExecutor] should be(true)
      import system.dispatcher

      val f = Future(()).flatMap { _ =>
        // this needs to be within an OnCompleteRunnable so that things are added to the batch
        val p = Future.successful(42)
        // we need the callback list to be non-empty when the blocking{} call is executing
        p.onComplete { _ =>
          ()
        }
        val r = p.map { _ =>
          // trigger the resubmitUnbatched() call
          blocking { () }
          // make sure that the other task runs to completion before continuing
          Thread.sleep(500)
          // now try again to blockOn()
          blocking { () }
        }
        p.onComplete { _ =>
          ()
        }
        r
      }
      Await.result(f, 3.seconds) should be(())
    }

    "work with tasks that block inside blocking" in {
      system.dispatcher.isInstanceOf[BatchingExecutor] should be(true)
      import system.dispatcher

      val f = Future(()).flatMap { _ =>
        blocking {
          blocking {
            blocking {
              Future.successful(42)
            }
          }
        }
      }
      Await.result(f, 3.seconds) should be(42)
    }

    "work with same-thread executor plus blocking" in {
      val ec = ExecutionContext.parasitic
      var x = 0
      ec.execute(new Runnable {
        override def run = {
          ec.execute(new Runnable {
            override def run = blocking {
              x = 1
            }
          })
        }
      })
      x should be(1)
    }

    "work with same-thread dispatcher plus blocking" in {
      val a = TestActorRef(Props(new Actor {
        def receive = {
          case msg =>
            blocking {
              sender() ! msg
            }
        }
      }))
      val b = TestActorRef(Props(new Actor {
        def receive = {
          case msg => a.forward(msg)
        }
      }))
      val p = TestProbe()
      p.send(b, "hello")
      p.expectMsg(0.seconds, "hello")
    }

    "work with same-thread dispatcher as executor with blocking" in {
      abstract class RunBatch extends Runnable with Batchable {
        override def isBatchable = true
      }
      val ec = system.dispatchers.lookup(CallingThreadDispatcher.Id)
      var x = 0
      ec.execute(new RunBatch {
        override def run = {
          // enqueue a task to the batch
          ec.execute(new RunBatch {
            override def run = blocking {
              x = 1
            }
          })
          // now run it
          blocking {
            ()
          }
        }
      })
      x should be(1)
    }
  }

  "A SerializedSuspendableExecutionContext" must {
    "be suspendable and resumable" in {
      val sec = SerializedSuspendableExecutionContext(1)(ExecutionContext.global)
      val counter = new AtomicInteger(0)
      def perform(f: Int => Int) = sec.execute(new Runnable { def run = counter.set(f(counter.get)) })
      perform(_ + 1)
      perform(x => { sec.suspend(); x * 2 })
      awaitCond(counter.get == 2)
      perform(_ + 4)
      perform(_ * 2)
      sec.size() should ===(2)
      Thread.sleep(500)
      sec.size() should ===(2)
      counter.get should ===(2)
      sec.resume()
      awaitCond(counter.get == 12)
      perform(_ * 2)
      awaitCond(counter.get == 24)
      sec.isEmpty should ===(true)
    }

    "execute 'throughput' number of tasks per sweep" in {
      val submissions = new AtomicInteger(0)
      val counter = new AtomicInteger(0)
      val underlying = new ExecutionContext {
        override def execute(r: Runnable): Unit = { submissions.incrementAndGet(); ExecutionContext.global.execute(r) }
        override def reportFailure(t: Throwable): Unit = { ExecutionContext.global.reportFailure(t) }
      }
      val throughput = 25
      val sec = SerializedSuspendableExecutionContext(throughput)(underlying)
      sec.suspend()
      def perform(f: Int => Int) = sec.execute(new Runnable { def run = counter.set(f(counter.get)) })

      val total = 1000
      (1 to total).foreach { _ =>
        perform(_ + 1)
      }
      sec.size() should ===(total)
      sec.resume()
      awaitCond(counter.get == total)
      submissions.get should ===(total / throughput)
      sec.isEmpty should ===(true)
    }

    "execute tasks in serial" in {
      val sec = SerializedSuspendableExecutionContext(1)(ExecutionContext.global)
      val total = 10000
      val counter = new AtomicInteger(0)
      def perform(f: Int => Int) = sec.execute(new Runnable { def run = counter.set(f(counter.get)) })

      (1 to total).foreach { i =>
        perform(c => if (c == (i - 1)) c + 1 else c)
      }
      awaitCond(counter.get == total)
      sec.isEmpty should ===(true)
    }

    "relinquish thread when suspended" in {
      val submissions = new AtomicInteger(0)
      val counter = new AtomicInteger(0)
      val underlying = new ExecutionContext {
        override def execute(r: Runnable): Unit = { submissions.incrementAndGet(); ExecutionContext.global.execute(r) }
        override def reportFailure(t: Throwable): Unit = { ExecutionContext.global.reportFailure(t) }
      }
      val throughput = 25
      val sec = SerializedSuspendableExecutionContext(throughput)(underlying)
      sec.suspend()
      def perform(f: Int => Int) = sec.execute(new Runnable { def run = counter.set(f(counter.get)) })
      perform(_ + 1)
      (1 to 10).foreach { _ =>
        perform(identity)
      }
      perform(x => { sec.suspend(); x * 2 })
      perform(_ + 8)
      sec.size() should ===(13)
      sec.resume()
      awaitCond(counter.get == 2)
      sec.resume()
      awaitCond(counter.get == 10)
      sec.isEmpty should ===(true)
      submissions.get should ===(2)
    }
  }
}
