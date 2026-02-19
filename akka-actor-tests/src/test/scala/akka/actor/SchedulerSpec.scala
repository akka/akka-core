/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.io.Closeable
import java.util.concurrent._
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

import atomic.{ AtomicInteger, AtomicReference }
import com.typesafe.config.{ Config, ConfigFactory }
import language.postfixOps
import org.scalatest.BeforeAndAfterEach

import akka.pattern.ask
import akka.testkit._

object SchedulerSpec {
  val testConfRevolver =
    ConfigFactory.parseString("""
    akka.scheduler.implementation = akka.actor.LightArrayRevolverScheduler
    akka.scheduler.ticks-per-wheel = 32
  """).withFallback(AkkaSpec.testConf)

}

trait SchedulerSpec extends BeforeAndAfterEach with DefaultTimeout with ImplicitSender { this: AkkaSpec =>
  import system.dispatcher

  def collectCancellable(c: Cancellable): Cancellable

  abstract class ScheduleAdapter {
    def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, runnable: Runnable)(
        implicit executor: ExecutionContext): Cancellable

    def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, receiver: ActorRef, message: Any)(
        implicit
        executor: ExecutionContext): Cancellable
  }

  class ScheduleWithFixedDelayAdapter extends ScheduleAdapter {
    def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, runnable: Runnable)(
        implicit executor: ExecutionContext): Cancellable =
      system.scheduler.scheduleWithFixedDelay(initialDelay, delay)(runnable)

    def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, receiver: ActorRef, message: Any)(
        implicit
        executor: ExecutionContext): Cancellable =
      system.scheduler.scheduleWithFixedDelay(initialDelay, delay, receiver, message)

    override def toString: String = "scheduleWithFixedDelay"
  }

  class ScheduleAtFixedRateAdapter extends ScheduleAdapter {
    def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, runnable: Runnable)(
        implicit executor: ExecutionContext): Cancellable =
      system.scheduler.scheduleAtFixedRate(initialDelay, delay)(runnable)

    def schedule(initialDelay: FiniteDuration, delay: FiniteDuration, receiver: ActorRef, message: Any)(
        implicit
        executor: ExecutionContext): Cancellable =
      system.scheduler.scheduleAtFixedRate(initialDelay, delay, receiver, message)

    override def toString: String = "scheduleAtFixedRate"
  }

  "A Scheduler" when {

    "using scheduleOnce" must {
      "schedule once" taggedAs TimingTest in {
        case object Tick
        val countDownLatch = new CountDownLatch(3)
        val tickActor = system.actorOf(Props(new Actor {
          def receive = { case Tick => countDownLatch.countDown() }
        }))

        // run after 300 millisec
        collectCancellable(system.scheduler.scheduleOnce(300.millis, tickActor, Tick))
        collectCancellable(system.scheduler.scheduleOnce(300.millis)(countDownLatch.countDown()))

        // should not be run immediately
        assert(countDownLatch.await(100, TimeUnit.MILLISECONDS) == false)
        countDownLatch.getCount should ===(3L)

        // after 1 second the wait should fail
        assert(countDownLatch.await(2, TimeUnit.SECONDS) == false)
        // should still be 1 left
        countDownLatch.getCount should ===(1L)
      }

      /**
       * ticket #372
       */
      "be cancellable" taggedAs TimingTest in {
        for (_ <- 1 to 10) system.scheduler.scheduleOnce(1 second, testActor, "fail").cancel()

        expectNoMessage(2 seconds)
      }

      "be canceled if cancel is performed before execution" taggedAs TimingTest in {
        val task = collectCancellable(system.scheduler.scheduleOnce(10 seconds)(()))
        task.cancel() should ===(true)
        task.isCancelled should ===(true)
        task.cancel() should ===(false)
        task.isCancelled should ===(true)
      }

      "not be canceled if cancel is performed after execution" taggedAs TimingTest in {
        val latch = TestLatch(1)
        val task = collectCancellable(system.scheduler.scheduleOnce(10.millis)(latch.countDown()))
        Await.ready(latch, remainingOrDefault)
        task.cancel() should ===(false)
        task.isCancelled should ===(false)
        task.cancel() should ===(false)
        task.isCancelled should ===(false)
      }

      "never fire prematurely" taggedAs TimingTest in {
        val ticks = new TestLatch(300)

        final case class Msg(ts: Long)

        val actor = system.actorOf(Props(new Actor {
          def receive = {
            case Msg(ts) =>
              val now = System.nanoTime
              // Make sure that no message has been dispatched before the scheduled time (10ms) has occurred
              if (now < ts) throw new RuntimeException("Interval is too small: " + (now - ts))
              ticks.countDown()
          }
        }))

        (1 to 300).foreach { _ =>
          collectCancellable(system.scheduler.scheduleOnce(20.millis, actor, Msg(System.nanoTime)))
          Thread.sleep(5)
        }

        Await.ready(ticks, 3 seconds)
      }

      "handle timeouts equal to multiple of wheel period" taggedAs TimingTest in {
        val timeout = 3200.millis
        val barrier = TestLatch()
        import system.dispatcher
        val job = system.scheduler.scheduleOnce(timeout)(barrier.countDown())
        try {
          Await.ready(barrier, 5000.millis)
        } finally {
          job.cancel()
        }
      }

      "survive being stressed without cancellation" taggedAs TimingTest in {
        val r = ThreadLocalRandom.current()
        val N = 100000
        for (_ <- 1 to N) {
          val next = r.nextInt(3000)
          val now = System.nanoTime
          system.scheduler.scheduleOnce(next.millis) {
            val stop = System.nanoTime
            testActor ! (stop - now - next * 1000000L)
          }
        }
        val latencies = within(10.seconds) {
          for (i <- 1 to N)
            yield try expectMsgType[Long]
            catch {
              case NonFatal(e) => throw new Exception(s"failed expecting the $i-th latency", e)
            }
        }
        val histogram = latencies.groupBy(_ / 100000000L)
        for (k <- histogram.keys.toSeq.sorted) {
          system.log.info(f"${k * 100}%3d: ${histogram(k).size}")
        }
      }
    }

    // same tests for fixedDelay and fixedRate
    List(new ScheduleWithFixedDelayAdapter, new ScheduleAtFixedRateAdapter).foreach { scheduleAdapter =>
      s"using $scheduleAdapter" must {

        "schedule more than once" taggedAs TimingTest in {
          case object Tick
          case object Tock

          val tickActor, tickActor2 = system.actorOf(Props(new Actor {
            var ticks = 0

            def receive = {
              case Tick =>
                if (ticks < 3) {
                  sender() ! Tock
                  ticks += 1
                }
            }
          }))
          // run every 50 milliseconds
          scheduleAdapter.schedule(Duration.Zero, 50.millis, tickActor, Tick)

          // after max 1 second it should be executed at least the 3 times already
          expectMsg(Tock)
          expectMsg(Tock)
          expectMsg(Tock)
          expectNoMessage(500.millis)

          collectCancellable(scheduleAdapter.schedule(Duration.Zero, 50.millis, () => tickActor2 ! Tick))

          // after max 1 second it should be executed at least the 3 times already
          expectMsg(Tock)
          expectMsg(Tock)
          expectMsg(Tock)
          expectNoMessage(500.millis)
        }

        "stop continuous scheduling if the receiving actor has been terminated" taggedAs TimingTest in {
          val actor = system.actorOf(Props(new Actor {
            def receive = {
              case x => sender() ! x
            }
          }))

          // run immediately and then every 100 milliseconds
          collectCancellable(scheduleAdapter.schedule(Duration.Zero, 100.millis, actor, "msg"))
          expectMsg("msg")

          // stop the actor and, hence, the continuous messaging from happening
          system.stop(actor)

          expectNoMessage(500.millis)
        }

        "stop continuous scheduling if the task throws exception" taggedAs TimingTest in {
          EventFilter[Exception]("TEST", occurrences = 1).intercept {
            val count = new AtomicInteger(0)
            collectCancellable(scheduleAdapter.schedule(Duration.Zero, 20.millis, () => {
              val c = count.incrementAndGet()
              testActor ! c
              if (c == 3) throw new RuntimeException("TEST") with NoStackTrace
            }))
            expectMsg(1)
            expectMsg(2)
            expectMsg(3)
            expectNoMessage(200.millis)
          }
        }

        "stop continuous scheduling if the task throws IllegalStateException" taggedAs TimingTest in {
          // when first throws
          EventFilter[Exception]("TEST-1", occurrences = 1).intercept {
            val count1 = new AtomicInteger(0)
            collectCancellable(scheduleAdapter.schedule(Duration.Zero, 20.millis, () => {
              val c = count1.incrementAndGet()
              if (c == 1)
                throw new IllegalStateException("TEST-1") with NoStackTrace
              else
                testActor ! c
            }))
            expectNoMessage(200.millis)
          }

          // when later
          EventFilter[Exception]("TEST-3", occurrences = 1).intercept {
            val count2 = new AtomicInteger(0)
            collectCancellable(scheduleAdapter.schedule(Duration.Zero, 20.millis, () => {
              val c = count2.incrementAndGet()
              testActor ! c
              if (c == 3) throw new IllegalStateException("TEST-3") with NoStackTrace
            }))
            expectMsg(1)
            expectMsg(2)
            expectMsg(3)
            expectNoMessage(200.millis)
          }
        }

        "be cancellable during initial delay" taggedAs TimingTest in {
          val ticks = new AtomicInteger

          val initialDelay = 200.millis.dilated
          val delay = 10.millis.dilated
          val timeout = collectCancellable(scheduleAdapter.schedule(initialDelay, delay, () => {
            ticks.incrementAndGet()
          }))
          Thread.sleep(10.millis.dilated.toMillis)
          timeout.cancel()
          Thread.sleep((initialDelay + 100.millis.dilated).toMillis)

          ticks.get should ===(0)
        }

        "be cancellable after initial delay" taggedAs TimingTest in {
          val ticks = new AtomicInteger

          val initialDelay = 90.millis.dilated
          val delay = 500.millis.dilated
          val timeout = collectCancellable(scheduleAdapter.schedule(initialDelay, delay, () => {
            ticks.incrementAndGet()
          }))
          Thread.sleep((initialDelay + 200.millis.dilated).toMillis)
          timeout.cancel()
          Thread.sleep((delay + 100.millis.dilated).toMillis)

          ticks.get should ===(1)
        }

        /**
         * ticket #307
         */
        "pick up schedule after actor restart" taggedAs TimingTest in {

          object Ping
          object Crash

          val restartLatch = new TestLatch
          val pingLatch = new TestLatch(6)

          val supervisor =
            system.actorOf(Props(new Supervisor(AllForOneStrategy(3, 1 second)(List(classOf[Exception])))))
          val props = Props(new Actor {
            def receive = {
              case Ping  => pingLatch.countDown()
              case Crash => throw new Exception("CRASH")
            }

            override def postRestart(reason: Throwable) = restartLatch.open()
          })
          val actor = Await.result((supervisor ? props).mapTo[ActorRef], timeout.duration)

          collectCancellable(scheduleAdapter.schedule(500.millis, 500.millis, actor, Ping))
          // appx 2 pings before crash
          EventFilter[Exception]("CRASH", occurrences = 1).intercept {
            collectCancellable(system.scheduler.scheduleOnce(1000.millis, actor, Crash))
          }

          Await.ready(restartLatch, 2 seconds)
          // should be enough time for the ping countdown to recover and reach 6 pings
          Await.ready(pingLatch, 5 seconds)
        }

        "schedule with different initial delay and frequency" taggedAs TimingTest in {
          val ticks = new TestLatch(3)

          case object Msg

          val actor = system.actorOf(Props(new Actor {
            def receive = { case Msg => ticks.countDown() }
          }))

          val startTime = System.nanoTime()
          collectCancellable(scheduleAdapter.schedule(1 second, 300.millis, actor, Msg))
          Await.ready(ticks, 3 seconds)

          // LARS is a bit more aggressive in scheduling recurring tasks at the right
          // frequency and may execute them a little earlier; the actual expected timing
          // is 1599ms on a fast machine or 1699ms on a loaded one (plus some room for jenkins)
          (System.nanoTime() - startTime).nanos.toMillis should ===(1750L +- 250)
        }
      }
    }

    "using scheduleAtFixedRate" must {

      "adjust for scheduler inaccuracy" taggedAs TimingTest in {
        val startTime = System.nanoTime
        val n = 200
        val latch = new TestLatch(n)
        system.scheduler.scheduleAtFixedRate(25.millis, 25.millis) { () =>
          latch.countDown()
        }
        Await.ready(latch, 6.seconds)
        // Rate
        n * 1000.0 / (System.nanoTime - startTime).nanos.toMillis should ===(40.0 +- 4)
      }

      "not be affected by long running task" taggedAs TimingTest in {
        val n = 22
        val latch = new TestLatch(n)
        val startTime = System.nanoTime
        system.scheduler.scheduleAtFixedRate(225.millis, 225.millis) { () =>
          Thread.sleep(100)
          latch.countDown()
        }
        Await.ready(latch, 6.seconds)
        // Rate
        n * 1000.0 / (System.nanoTime - startTime).nanos.toMillis should ===(4.4 +- 0.5)
      }
    }

    "using scheduleWithFixedDelay" must {

      "keep delay after task completed" taggedAs TimingTest in {
        val n = 12
        val latch = new TestLatch(n)
        val startTime = System.nanoTime
        system.scheduler.scheduleWithFixedDelay(125.millis, 125.millis) { () =>
          Thread.sleep(100)
          latch.countDown()
        }
        Await.ready(latch, 6.seconds)
        // Rate
        n * 1000.0 / (System.nanoTime - startTime).nanos.toMillis should ===(4.4 +- 0.5)
      }
    }

  }
}

class LightArrayRevolverSchedulerSpec extends AkkaSpec(SchedulerSpec.testConfRevolver) with SchedulerSpec {

  def collectCancellable(c: Cancellable): Cancellable = c

  def tickDuration = system.scheduler.asInstanceOf[LightArrayRevolverScheduler].TickDuration

  "A LightArrayRevolverScheduler" when {

    "using scheduleOnce" must {

      "reject tasks scheduled too far into the future" taggedAs TimingTest in {
        val maxDelay = tickDuration * Int.MaxValue
        import system.dispatcher
        system.scheduler.scheduleOnce(maxDelay, testActor, "OK")
        intercept[IllegalArgumentException] {
          system.scheduler.scheduleOnce(maxDelay + tickDuration, testActor, "Too far")
        }
      }

      "survive being stressed with cancellation" taggedAs TimingTest in {
        import system.dispatcher
        val r = ThreadLocalRandom.current
        val N = 1000000
        val tasks = for (_ <- 1 to N) yield {
          val next = r.nextInt(3000)
          val now = System.nanoTime
          system.scheduler.scheduleOnce(next.millis) {
            val stop = System.nanoTime
            testActor ! (stop - now - next * 1000000L)
          }
        }
        // get somewhat into the middle of things
        Thread.sleep(500)
        val cancellations = for (t <- tasks) yield {
          t.cancel()
          if (t.isCancelled) 1 else 0
        }
        val cancelled = cancellations.sum
        println(cancelled)
        val latencies = within(10.seconds) {
          for (i <- 1 to (N - cancelled))
            yield try expectMsgType[Long]
            catch {
              case NonFatal(e) => throw new Exception(s"failed expecting the $i-th latency", e)
            }
        }
        val histogram = latencies.groupBy(_ / 100000000L)
        for (k <- histogram.keys.toSeq.sorted) {
          system.log.info(f"${k * 100}%3d: ${histogram(k).size}")
        }
        expectNoMessage(1.second)
      }

      "survive vicious enqueueing" taggedAs TimingTest in {
        withScheduler(config = ConfigFactory.parseString("akka.scheduler.ticks-per-wheel=2")) { (sched, driver) =>
          import driver._
          import system.dispatcher
          val counter = new AtomicInteger
          val terminated = Future {
            var rounds = 0
            while (Try(sched.scheduleOnce(Duration.Zero, new Scheduler.TaskRunOnClose {
                     override def run(): Unit = ()
                   })(localEC)).isSuccess) {
              Thread.sleep(1)
              driver.wakeUp(step)
              rounds += 1
            }
            rounds
          }
          def delay = if (ThreadLocalRandom.current.nextBoolean) step * 2 else step
          val N = 1000000
          (1 to N).foreach(_ =>
            sched.scheduleOnce(delay, new Scheduler.TaskRunOnClose {
              override def run(): Unit = counter.incrementAndGet()
            }))
          sched.close()
          Await.result(terminated, 3.seconds.dilated) should be > 10
          awaitAssert(counter.get should ===(N))
        }
      }

      "execute multiple jobs at once when expiring multiple buckets" taggedAs TimingTest in {
        withScheduler() { (sched, driver) =>
          implicit def ec: ExecutionContext = localEC
          import driver._
          val start = step / 2
          (0 to 3).foreach(i => sched.scheduleOnce(start + step * i, testActor, "hello"))
          expectNoMessage(step)
          wakeUp(step)
          expectWait(step)
          wakeUp(step * 4 + step / 2)
          expectWait(step / 2)
          (0 to 3).foreach(_ => expectMsg(Duration.Zero, "hello"))
        }
      }

      "properly defer jobs even when the timer thread oversleeps" taggedAs TimingTest in {
        withScheduler() { (sched, driver) =>
          implicit def ec: ExecutionContext = localEC
          import driver._
          sched.scheduleOnce(step * 3, probe.ref, "hello")
          wakeUp(step * 5)
          expectWait(step)
          wakeUp(step * 2)
          expectWait(step)
          wakeUp(step)
          probe.expectMsg("hello")
          expectWait(step)
        }
      }

      "correctly wrap around wheel rounds" taggedAs TimingTest in {
        withScheduler(config = ConfigFactory.parseString("akka.scheduler.ticks-per-wheel=2")) { (sched, driver) =>
          implicit def ec: ExecutionContext = localEC
          import driver._
          val start = step / 2
          (0 to 3).foreach(i => sched.scheduleOnce(start + step * i, probe.ref, "hello"))
          probe.expectNoMessage(step)
          wakeUp(step)
          expectWait(step)
          // the following are no for-comp to see which iteration fails
          wakeUp(step)
          probe.expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          probe.expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          probe.expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          probe.expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          expectWait(step)
        }
      }

      "correctly execute jobs when clock wraps around" taggedAs TimingTest in {
        withScheduler(Long.MaxValue - 200000000L) { (sched, driver) =>
          implicit def ec: ExecutionContext = localEC
          import driver._
          val start = step / 2
          (0 to 3).foreach(i => sched.scheduleOnce(start + step * i, testActor, "hello"))
          expectNoMessage(step)
          wakeUp(step)
          expectWait(step)
          // the following are no for-comp to see which iteration fails
          wakeUp(step)
          expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          expectMsg("hello")
          expectWait(step)
          wakeUp(step)
          expectWait(step)
        }
      }

      "correctly wrap around ticks" taggedAs TimingTest in {
        val numEvents = 40
        val targetTicks = Int.MaxValue - numEvents + 20

        withScheduler(_startTick = Int.MaxValue - 100) { (sched, driver) =>
          implicit def ec: ExecutionContext = localEC
          import driver._

          val start = step / 2

          wakeUp(step * targetTicks)
          probe.expectMsgType[Long]

          val nums = 0 until numEvents
          nums.foreach(i => sched.scheduleOnce(start + step * i, testActor, "hello-" + i))
          expectNoMessage(step)
          wakeUp(step)
          expectWait(step)

          nums.foreach { i =>
            wakeUp(step)
            expectMsg("hello-" + i)
            expectWait(step)
          }
        }
      }

      "reliably reject jobs when shutting down" taggedAs TimingTest in {
        withScheduler() { (sched, driver) =>
          import system.dispatcher
          val counter = new AtomicInteger
          Future { Thread.sleep(5); driver.close(); sched.close() }
          val headroom = 200
          var overrun = headroom
          val cap = 1000000
          val (success, failure) = Iterator
            .continually(Try(sched.scheduleOnce(100.millis, new Scheduler.TaskRunOnClose {
              override def run(): Unit = counter.incrementAndGet()
            })))
            .take(cap)
            .takeWhile(_.isSuccess || { overrun -= 1; overrun >= 0 })
            .partition(_.isSuccess)
          val s = success.size
          s should be < cap
          awaitCond(s == counter.get, message = s"$s was not ${counter.get}")
          failure.size should ===(headroom)
        }
      }

      "run TaskRunOnClose when Scheduler is closed" in {
        withScheduler() { (sched, driver) =>
          import system.dispatcher
          val counter = new AtomicInteger()
          sched.scheduleOnce(10.seconds)(counter.incrementAndGet())
          sched.scheduleOnce(10.seconds, new Scheduler.TaskRunOnClose {
            override def run(): Unit = counter.incrementAndGet()
          })
          driver.close()
          sched.close()
          counter.get should ===(1)
        }
      }
    }

    // same tests for fixedDelay and fixedRate
    List(new ScheduleWithFixedDelayAdapter, new ScheduleAtFixedRateAdapter).foreach { scheduleAdapter =>
      s"using $scheduleAdapter" must {

        "reject periodic tasks scheduled too far into the future" taggedAs TimingTest in {
          val maxDelay = tickDuration * Int.MaxValue
          import system.dispatcher
          scheduleAdapter.schedule(maxDelay, 1.second, testActor, "OK")
          intercept[IllegalArgumentException] {
            scheduleAdapter.schedule(maxDelay + tickDuration, 1.second, testActor, "Too far")
          }
        }

        "reject periodic tasks scheduled with too long interval" taggedAs TimingTest in {
          val maxDelay = tickDuration * Int.MaxValue
          import system.dispatcher
          scheduleAdapter.schedule(100.millis, maxDelay, testActor, "OK")
          expectMsg("OK")
          intercept[IllegalArgumentException] {
            scheduleAdapter.schedule(100.millis, maxDelay + tickDuration, testActor, "Too long")
          }
          expectNoMessage(1.second)
        }
      }
    }

  }

  trait Driver {
    def wakeUp(d: FiniteDuration): Unit
    def expectWait(): FiniteDuration
    def expectWait(d: FiniteDuration): Unit = { expectWait() should ===(d) }
    def probe: TestProbe
    def step: FiniteDuration
    def close(): Unit
  }

  val localEC = new ExecutionContext {
    def execute(runnable: Runnable): Unit = { runnable.run() }
    def reportFailure(t: Throwable): Unit = { t.printStackTrace() }
  }

  def withScheduler(start: Long = 0L, _startTick: Int = 0, config: Config = ConfigFactory.empty)(
      thunk: (Scheduler with Closeable, Driver) => Unit): Unit = {
    val lbq = new AtomicReference[LinkedBlockingQueue[Long]](new LinkedBlockingQueue[Long])
    val prb = TestProbe()
    val tf = system.asInstanceOf[ActorSystemImpl].threadFactory

    @volatile var time: Long = start
    val sched = new LightArrayRevolverScheduler(config.withFallback(system.settings.config), log, tf) {
      override protected def clock(): Long = {
        // println(s"clock=$time")
        time
      }

      override protected def getShutdownTimeout: FiniteDuration = (10 seconds).dilated

      override protected def waitNanos(ns: Long): Unit = {
        // println(s"waiting $ns")
        prb.ref ! ns
        try time += (lbq.get match {
            case q: LinkedBlockingQueue[Long] => q.take()
            case null                         => 0L
          })
        catch {
          case _: InterruptedException => Thread.currentThread.interrupt()
        }
      }

      override protected def startTick: Int = _startTick
    }

    val driver = new Driver {
      def wakeUp(d: FiniteDuration) = lbq.get match {
        case q: LinkedBlockingQueue[Long] => q.offer(d.toNanos)
        case null                         =>
      }
      def expectWait(): FiniteDuration = probe.expectMsgType[Long].nanos
      def probe = prb
      def step = sched.TickDuration
      def close() = lbq.getAndSet(null) match {
        case q: LinkedBlockingQueue[Long] => q.offer(0L)
        case null                         =>
      }
    }
    driver.expectWait()
    try thunk(sched, driver)
    catch {
      case NonFatal(ex) =>
        try {
          driver.close()
          sched.close()
        } catch { case _: Exception => }
        throw ex
    }
    driver.close()
    sched.close()
  }

}
