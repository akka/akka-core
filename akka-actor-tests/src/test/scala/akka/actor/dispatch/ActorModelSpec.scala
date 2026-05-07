/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dispatch

import java.rmi.RemoteException
import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicLong }

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import com.typesafe.config.Config
import language.postfixOps
import org.scalatest.Assertions._

import akka.actor._
import akka.dispatch._
import akka.event.Logging.Error
import akka.pattern.ask
import akka.testkit._
import akka.util.Switch

object ActorModelSpec {

  sealed trait ActorModelMessage extends NoSerializationVerificationNeeded

  final case class TryReply(expect: Any) extends ActorModelMessage

  final case class Reply(expect: Any) extends ActorModelMessage

  final case class Forward(to: ActorRef, msg: Any) extends ActorModelMessage

  final case class CountDown(latch: CountDownLatch) extends ActorModelMessage

  final case class Increment(counter: AtomicLong) extends ActorModelMessage

  final case class AwaitLatch(latch: CountDownLatch) extends ActorModelMessage

  final case class Meet(acknowledge: CountDownLatch, waitFor: CountDownLatch) extends ActorModelMessage

  final case class CountDownNStop(latch: CountDownLatch) extends ActorModelMessage

  final case class Wait(time: Long) extends ActorModelMessage

  final case class WaitAck(time: Long, latch: CountDownLatch) extends ActorModelMessage

  case object Interrupt extends ActorModelMessage

  final case class InterruptNicely(expect: Any) extends ActorModelMessage

  case object Restart extends ActorModelMessage

  case object DoubleStop extends ActorModelMessage

  final case class ThrowException(e: Throwable) extends ActorModelMessage

  val Ping = "Ping"
  val Pong = "Pong"

  class DispatcherActor extends Actor {
    private val busy = new Switch(false)

    def interceptor = context.dispatcher.asInstanceOf[MessageDispatcherInterceptor]

    def ack(): Unit = {
      if (!busy.switchOn(())) {
        throw new Exception("isolation violated")
      } else {
        interceptor.getStats(self).msgsProcessed.incrementAndGet()
      }
    }

    override def postRestart(reason: Throwable): Unit = {
      interceptor.getStats(self).restarts.incrementAndGet()
    }

    def receive = {
      case AwaitLatch(latch) => { ack(); latch.await(); busy.switchOff(()) }
      case Meet(sign, wait)  => { ack(); sign.countDown(); wait.await(); busy.switchOff(()) }
      case Wait(time)        => { ack(); Thread.sleep(time); busy.switchOff(()) }
      case WaitAck(time, l)  => { ack(); Thread.sleep(time); l.countDown(); busy.switchOff(()) }
      case Reply(msg)        => { ack(); sender() ! msg; busy.switchOff(()) }
      case TryReply(msg)     => { ack(); sender().tell(msg, null); busy.switchOff(()) }
      case Forward(to, msg)  => { ack(); to.forward(msg); busy.switchOff(()) }
      case CountDown(latch)  => { ack(); latch.countDown(); busy.switchOff(()) }
      case Increment(count)  => { ack(); count.incrementAndGet(); busy.switchOff(()) }
      case CountDownNStop(l) => { ack(); l.countDown(); context.stop(self); busy.switchOff(()) }
      case Restart           => { ack(); busy.switchOff(()); throw new Exception("Restart requested") }
      case Interrupt => {
        ack(); sender() ! Status.Failure(new ActorInterruptedException(new InterruptedException("Ping!")));
        busy.switchOff(()); throw new InterruptedException("Ping!")
      }
      case InterruptNicely(msg)         => { ack(); sender() ! msg; busy.switchOff(()); Thread.currentThread().interrupt() }
      case ThrowException(e: Throwable) => { ack(); busy.switchOff(()); throw e }
      case DoubleStop                   => { ack(); context.stop(self); context.stop(self); busy.switchOff }
    }
  }

  class InterceptorStats {
    val suspensions = new AtomicLong(0)
    val resumes = new AtomicLong(0)
    val registers = new AtomicLong(0)
    val unregisters = new AtomicLong(0)
    val msgsReceived = new AtomicLong(0)
    val msgsProcessed = new AtomicLong(0)
    val restarts = new AtomicLong(0)
    override def toString =
      "InterceptorStats(susp=" + suspensions +
      ",res=" + resumes + ",reg=" + registers + ",unreg=" + unregisters +
      ",recv=" + msgsReceived + ",proc=" + msgsProcessed + ",restart=" + restarts
  }

  trait MessageDispatcherInterceptor extends MessageDispatcher {
    val stats = new ConcurrentHashMap[ActorRef, InterceptorStats]
    val stops = new AtomicLong(0)

    def getStats(actorRef: ActorRef) = {
      stats.get(actorRef) match {
        case null =>
          val is = new InterceptorStats
          stats.putIfAbsent(actorRef, is) match {
            case null  => is
            case other => other
          }
        case existing => existing
      }

    }

    protected[akka] abstract override def suspend(actor: ActorCell): Unit = {
      getStats(actor.self).suspensions.incrementAndGet()
      super.suspend(actor)
    }

    protected[akka] abstract override def resume(actor: ActorCell): Unit = {
      super.resume(actor)
      getStats(actor.self).resumes.incrementAndGet()
    }

    protected[akka] abstract override def register(actor: ActorCell): Unit = {
      assert(getStats(actor.self).registers.incrementAndGet() == 1)
      super.register(actor)
    }

    protected[akka] abstract override def unregister(actor: ActorCell): Unit = {
      assert(getStats(actor.self).unregisters.incrementAndGet() == 1)
      super.unregister(actor)
    }

    protected[akka] abstract override def dispatch(receiver: ActorCell, invocation: Envelope): Unit = {
      val stats = getStats(receiver.self)
      stats.msgsReceived.incrementAndGet()
      super.dispatch(receiver, invocation)
    }

    protected[akka] abstract override def shutdown(): Unit = {
      stops.incrementAndGet()
      super.shutdown()
    }
  }

  def assertDispatcher(dispatcher: MessageDispatcherInterceptor)(stops: Long = dispatcher.stops.get())(
      implicit system: ActorSystem): Unit = {
    val deadline = System.currentTimeMillis + dispatcher.shutdownTimeout.toMillis * 5
    try {
      await(deadline)(stops == dispatcher.stops.get)
    } catch {
      case e: Throwable =>
        system.eventStream.publish(
          Error(
            e,
            dispatcher.toString,
            dispatcher.getClass,
            "actual: stops=" + dispatcher.stops.get +
            " required: stops=" + stops))
        throw e
    }
  }

  def assertCountDown(latch: CountDownLatch, wait: Long, hint: String): Unit = {
    if (!latch.await(wait, TimeUnit.MILLISECONDS))
      fail("Failed to count down within " + wait + " millis (count at " + latch.getCount + "). " + hint)
  }

  def assertNoCountDown(latch: CountDownLatch, wait: Long, hint: String): Unit = {
    if (latch.await(wait, TimeUnit.MILLISECONDS))
      fail("Expected count down to fail after " + wait + " millis. " + hint)
  }

  def statsFor(actorRef: ActorRef, dispatcher: MessageDispatcher = null) =
    dispatcher.asInstanceOf[MessageDispatcherInterceptor].getStats(actorRef)

  def assertRefDefaultZero(actorRef: ActorRef, dispatcher: MessageDispatcher = null)(
      suspensions: Long = 0,
      resumes: Long = 0,
      registers: Long = 0,
      unregisters: Long = 0,
      msgsReceived: Long = 0,
      msgsProcessed: Long = 0,
      restarts: Long = 0)(implicit system: ActorSystem): Unit = {
    assertRef(actorRef, dispatcher)(suspensions, resumes, registers, unregisters, msgsReceived, msgsProcessed, restarts)
  }

  def assertRef(actorRef: ActorRef, dispatcher: MessageDispatcher = null)(
      suspensions: Long = statsFor(actorRef, dispatcher).suspensions.get(),
      resumes: Long = statsFor(actorRef, dispatcher).resumes.get(),
      registers: Long = statsFor(actorRef, dispatcher).registers.get(),
      unregisters: Long = statsFor(actorRef, dispatcher).unregisters.get(),
      msgsReceived: Long = statsFor(actorRef, dispatcher).msgsReceived.get(),
      msgsProcessed: Long = statsFor(actorRef, dispatcher).msgsProcessed.get(),
      restarts: Long = statsFor(actorRef, dispatcher).restarts.get())(implicit system: ActorSystem): Unit = {
    val stats = statsFor(
      actorRef,
      Option(dispatcher).getOrElse(
        actorRef.asInstanceOf[ActorRefWithCell].underlying.asInstanceOf[ActorCell].dispatcher))
    val deadline = System.currentTimeMillis + 1000
    try {
      await(deadline)(stats.suspensions.get() == suspensions)
      await(deadline)(stats.resumes.get() == resumes)
      await(deadline)(stats.registers.get() == registers)
      await(deadline)(stats.unregisters.get() == unregisters)
      await(deadline)(stats.msgsReceived.get() == msgsReceived)
      await(deadline)(stats.msgsProcessed.get() == msgsProcessed)
      await(deadline)(stats.restarts.get() == restarts)
    } catch {
      case e: Throwable =>
        system.eventStream.publish(
          Error(
            e,
            Option(dispatcher).toString,
            Option[Any](dispatcher).getOrElse(this).getClass,
            "actual: " + stats + ", required: InterceptorStats(susp=" + suspensions +
            ",res=" + resumes + ",reg=" + registers + ",unreg=" + unregisters +
            ",recv=" + msgsReceived + ",proc=" + msgsProcessed + ",restart=" + restarts))
        throw e
    }
  }

  @tailrec def await(until: Long)(condition: => Boolean): Unit =
    if (System.currentTimeMillis() <= until) {
      var done = false
      try {
        done = condition
        if (!done) Thread.sleep(25)
      } catch {
        case _: InterruptedException =>
      }
      if (!done) await(until)(condition)
    } else throw new AssertionError("await failed")
}

abstract class ActorModelSpec(config: String) extends AkkaSpec(config) with DefaultTimeout {

  import ActorModelSpec._

  def newTestActor(dispatcher: String) = system.actorOf(Props[DispatcherActor]().withDispatcher(dispatcher))

  def awaitStarted(ref: ActorRef): Unit = {
    awaitCond(ref match {
      case r: RepointableRef => r.isStarted
      case _                 => true
    }, 1 second, 10 millis)
  }

  protected def interceptedDispatcher(): MessageDispatcherInterceptor
  protected def dispatcherType: String

  "A " + dispatcherType must {

    "dynamically handle its own life cycle" in {
      implicit val dispatcher = interceptedDispatcher()
      assertDispatcher(dispatcher)(stops = 0)
      val a = newTestActor(dispatcher.id)
      assertDispatcher(dispatcher)(stops = 0)
      system.stop(a)
      assertDispatcher(dispatcher)(stops = 1)
      assertRef(a, dispatcher)(
        suspensions = 0,
        resumes = 0,
        registers = 1,
        unregisters = 1,
        msgsReceived = 0,
        msgsProcessed = 0,
        restarts = 0)

      for (i <- 1 to 10) yield Future { i }
      assertDispatcher(dispatcher)(stops = 2)

      val a2 = newTestActor(dispatcher.id)
      for (i <- 1 to 10) yield Future { i }

      assertDispatcher(dispatcher)(stops = 2)

      system.stop(a2)
      assertDispatcher(dispatcher)(stops = 3)
    }

    "process messages one at a time" in {
      implicit val dispatcher = interceptedDispatcher()
      val start, oneAtATime = new CountDownLatch(1)
      val a = newTestActor(dispatcher.id)
      awaitStarted(a)

      a ! CountDown(start)
      assertCountDown(start, 3.seconds.dilated.toMillis, "Should process first message within 3 seconds")
      assertRefDefaultZero(a)(registers = 1, msgsReceived = 1, msgsProcessed = 1)

      a ! Wait(1000)
      a ! CountDown(oneAtATime)
      // in case of serialization violation, restart would happen instead of count down
      assertCountDown(oneAtATime, (1.5 seconds).dilated.toMillis, "Processed message when allowed")
      assertRefDefaultZero(a)(registers = 1, msgsReceived = 3, msgsProcessed = 3)

      system.stop(a)
      assertRefDefaultZero(a)(registers = 1, unregisters = 1, msgsReceived = 3, msgsProcessed = 3)
    }

    "handle queueing from multiple threads" in {
      implicit val dispatcher = interceptedDispatcher()
      val counter = new CountDownLatch(200)
      val a = newTestActor(dispatcher.id)

      for (_ <- 1 to 10) {
        spawn {
          for (_ <- 1 to 20) {
            a ! WaitAck(1, counter)
          }
        }
      }
      assertCountDown(counter, 3.seconds.dilated.toMillis, "Should process 200 messages")
      assertRefDefaultZero(a)(registers = 1, msgsReceived = 200, msgsProcessed = 200)

      system.stop(a)
    }

    def spawn(f: => Unit): Unit = {
      (new Thread {
        override def run(): Unit =
          try f
          catch {
            case e: Throwable => system.eventStream.publish(Error(e, "spawn", this.getClass, "error in spawned thread"))
          }
      }).start()
    }

    "not process messages for a suspended actor" in {
      implicit val dispatcher = interceptedDispatcher()
      val a = newTestActor(dispatcher.id).asInstanceOf[InternalActorRef]
      awaitStarted(a)
      val done = new CountDownLatch(1)
      a.suspend()
      a ! CountDown(done)
      assertNoCountDown(done, 1000, "Should not process messages while suspended")
      assertRefDefaultZero(a)(registers = 1, msgsReceived = 1, suspensions = 1)

      a.resume(causedByFailure = null)
      assertCountDown(done, 3.seconds.dilated.toMillis, "Should resume processing of messages when resumed")
      assertRefDefaultZero(a)(registers = 1, msgsReceived = 1, msgsProcessed = 1, suspensions = 1, resumes = 1)

      system.stop(a)
      assertRefDefaultZero(a)(
        registers = 1,
        unregisters = 1,
        msgsReceived = 1,
        msgsProcessed = 1,
        suspensions = 1,
        resumes = 1)
    }

    "handle waves of actors" in {
      val dispatcher = interceptedDispatcher()
      val props = Props[DispatcherActor]().withDispatcher(dispatcher.id)

      def flood(num: Int): Unit = {
        val cachedMessage = CountDownNStop(new CountDownLatch(num))
        val stopLatch = new CountDownLatch(num)
        val keepAliveLatch = new CountDownLatch(1)
        val waitTime = (20 seconds).dilated.toMillis
        val boss = system.actorOf(Props(new Actor {
          def receive = {
            case "run"         => for (_ <- 1 to num) context.watch(context.actorOf(props)) ! cachedMessage
            case Terminated(_) => stopLatch.countDown()
          }
        }).withDispatcher("boss"))
        try {
          // this future is meant to keep the dispatcher alive until the end of the test run even if
          // the boss doesn't create children fast enough to keep the dispatcher from becoming empty
          // and it needs to be on a separate thread to not deadlock the calling thread dispatcher
          new Thread(new Runnable {
            def run() =
              Future {
                keepAliveLatch.await(waitTime, TimeUnit.MILLISECONDS)
              }(dispatcher)
          }).start()
          boss ! "run"
          assertCountDown(cachedMessage.latch, waitTime, "Counting down from " + num)
          assertCountDown(stopLatch, waitTime, "Expected all children to stop")
        } finally {
          keepAliveLatch.countDown()
          system.stop(boss)
        }
      }
      for (run <- 1 to 3) {
        flood(10000)
        assertDispatcher(dispatcher)(stops = run)
      }
    }

    "continue to process messages when a thread gets interrupted and throws an exception" in {
      filterEvents(
        EventFilter[InterruptedException](),
        EventFilter[ActorInterruptedException](),
        EventFilter[akka.event.Logging.LoggerException]()) {
        implicit val dispatcher = interceptedDispatcher()
        val a = newTestActor(dispatcher.id)
        val f1 = a ? Reply("foo")
        val f2 = a ? Reply("bar")
        val f3 = a ? Interrupt
        Thread.interrupted() // CallingThreadDispatcher may necessitate this
        val f4 = a ? Reply("foo2")
        val f5 = a ? Interrupt
        Thread.interrupted() // CallingThreadDispatcher may necessitate this
        val f6 = a ? Reply("bar2")

        val c = system.scheduler.scheduleOnce(2.seconds) {
          import scala.jdk.CollectionConverters._
          Thread.getAllStackTraces().asScala.foreach {
            case (thread, stack) =>
              println(s"$thread:")
              stack.foreach(s => println(s"\t$s"))
          }
        }
        assert(Await.result(f1, timeout.duration) === "foo")
        assert(Await.result(f2, timeout.duration) === "bar")
        assert(Await.result(f4, timeout.duration) === "foo2")
        assert(intercept[ActorInterruptedException](Await.result(f3, timeout.duration)).getCause.getMessage === "Ping!")
        assert(Await.result(f6, timeout.duration) === "bar2")
        assert(intercept[ActorInterruptedException](Await.result(f5, timeout.duration)).getCause.getMessage === "Ping!")
        c.cancel()
        Thread.sleep(300) // give the EventFilters a chance of catching all messages
      }
    }

    "continue to process messages without failure when a thread gets interrupted and doesn't throw an exception" in {
      filterEvents(EventFilter[InterruptedException]()) {
        implicit val dispatcher = interceptedDispatcher()
        val a = newTestActor(dispatcher.id)
        val f1 = a ? Reply("foo")
        val f2 = a ? Reply("bar")
        val f3 = a ? InterruptNicely("baz")
        val f4 = a ? Reply("foo2")
        val f5 = a ? InterruptNicely("baz2")
        val f6 = a ? Reply("bar2")

        assert(Await.result(f1, timeout.duration) === "foo")
        assert(Await.result(f2, timeout.duration) === "bar")
        assert(Await.result(f3, timeout.duration) === "baz")
        assert(Await.result(f4, timeout.duration) === "foo2")
        assert(Await.result(f5, timeout.duration) === "baz2")
        assert(Await.result(f6, timeout.duration) === "bar2")
        // clear the interrupted flag (only needed for the CallingThreadDispatcher) so the next test can continue normally
        Thread.interrupted()
      }
    }

    "continue to process messages when exception is thrown" in {
      filterEvents(EventFilter[IndexOutOfBoundsException](), EventFilter[RemoteException]()) {
        implicit val dispatcher = interceptedDispatcher()
        val a = newTestActor(dispatcher.id)
        val f1 = a ? Reply("foo")
        val f2 = a ? Reply("bar")
        val f3 = a ? ThrowException(new IndexOutOfBoundsException("IndexOutOfBoundsException"))
        val f4 = a ? Reply("foo2")
        val f5 = a ? ThrowException(new RemoteException("RemoteException"))
        val f6 = a ? Reply("bar2")

        assert(Await.result(f1, timeout.duration) === "foo")
        assert(Await.result(f2, timeout.duration) === "bar")
        assert(Await.result(f4, timeout.duration) === "foo2")
        assert(Await.result(f6, timeout.duration) === "bar2")
        assert(f3.value.isEmpty)
        assert(f5.value.isEmpty)
      }
    }

    "not double-deregister" in {
      implicit val dispatcher = interceptedDispatcher()
      for (_ <- 1 to 1000) system.actorOf(Props.empty)
      val a = newTestActor(dispatcher.id)
      a ! DoubleStop
      awaitCond(statsFor(a, dispatcher).registers.get == 1)
      awaitCond(statsFor(a, dispatcher).unregisters.get == 1)
    }
  }
}

object DispatcherModelSpec {
  import ActorModelSpec._

  val config = {
    """
      boss {
        executor = thread-pool-executor
        type = PinnedDispatcher
      }
    """ +
    // use unique dispatcher id for each test, since MessageDispatcherInterceptor holds state
    (for (n <- 1 to 30) yield """
        test-dispatcher-%s {
          type = "akka.actor.dispatch.DispatcherModelSpec$MessageDispatcherInterceptorConfigurator"
        }""".format(n)).mkString
  }

  class MessageDispatcherInterceptorConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends MessageDispatcherConfigurator(config, prerequisites) {

    import akka.util.Helpers.ConfigOps

    private val instance: MessageDispatcher =
      new Dispatcher(
        this,
        config.getString("id"),
        config.getInt("throughput"),
        config.getNanosDuration("throughput-deadline-time"),
        configureExecutor(),
        config.getMillisDuration("shutdown-timeout")) with MessageDispatcherInterceptor

    override def dispatcher(): MessageDispatcher = instance
  }
}

class DispatcherModelSpec extends ActorModelSpec(DispatcherModelSpec.config) {
  import ActorModelSpec._

  val dispatcherCount = new AtomicInteger()

  override def interceptedDispatcher(): MessageDispatcherInterceptor = {
    // use new id for each test, since the MessageDispatcherInterceptor holds state
    system.dispatchers
      .lookup("test-dispatcher-" + dispatcherCount.incrementAndGet())
      .asInstanceOf[MessageDispatcherInterceptor]
  }

  override def dispatcherType = "Dispatcher"

  "A " + dispatcherType must {
    "process messages in parallel" in {
      val probeA, probeB = TestProbe()
      implicit val dispatcher = interceptedDispatcher()
      val aStart, aStop, bParallel = new CountDownLatch(1)
      val a, b = newTestActor(dispatcher.id)
      probeA.watch(a)
      probeB.watch(b)

      a ! Meet(aStart, aStop)
      assertCountDown(aStart, 3.seconds.dilated.toMillis, "Should process first message within 3 seconds")

      b ! CountDown(bParallel)
      assertCountDown(bParallel, 3.seconds.dilated.toMillis, "Should process other actors in parallel")

      aStop.countDown()

      system.stop(a)
      system.stop(b)

      probeA.expectTerminated(a)
      probeB.expectTerminated(b)

      assertRefDefaultZero(a)(registers = 1, unregisters = 1, msgsReceived = 1, msgsProcessed = 1)
      assertRefDefaultZero(b)(registers = 1, unregisters = 1, msgsReceived = 1, msgsProcessed = 1)
    }
  }
}

@nowarn
object BalancingDispatcherModelSpec {
  import ActorModelSpec._

  // TODO check why throughput=1 here? (came from old test)
  val config = {
    """
      boss {
        executor = thread-pool-executor
        type = PinnedDispatcher
      }
    """ +
    // use unique dispatcher id for each test, since MessageDispatcherInterceptor holds state
    (for (n <- 1 to 30)
      yield """
        test-balancing-dispatcher-%s {
          type = "akka.actor.dispatch.BalancingDispatcherModelSpec$BalancingMessageDispatcherInterceptorConfigurator"
          throughput=1
        }""".format(n)).mkString
  }

  class BalancingMessageDispatcherInterceptorConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends BalancingDispatcherConfigurator(config, prerequisites) {

    import akka.util.Helpers.ConfigOps

    override protected def create(mailboxType: MailboxType): BalancingDispatcher =
      new BalancingDispatcher(
        this,
        config.getString("id"),
        config.getInt("throughput"),
        config.getNanosDuration("throughput-deadline-time"),
        mailboxType,
        configureExecutor(),
        config.getMillisDuration("shutdown-timeout"),
        config.getBoolean("attempt-teamwork")) with MessageDispatcherInterceptor
  }
}

class BalancingDispatcherModelSpec extends ActorModelSpec(BalancingDispatcherModelSpec.config) {
  import ActorModelSpec._

  val dispatcherCount = new AtomicInteger()

  override def interceptedDispatcher(): MessageDispatcherInterceptor = {
    // use new id for each test, since the MessageDispatcherInterceptor holds state
    system.dispatchers
      .lookup("test-balancing-dispatcher-" + dispatcherCount.incrementAndGet())
      .asInstanceOf[MessageDispatcherInterceptor]
  }

  override def dispatcherType = "Balancing Dispatcher"

  "A " + dispatcherType must {
    "process messages in parallel" in {
      implicit val dispatcher = interceptedDispatcher()
      val aStart, aStop, bParallel = new CountDownLatch(1)
      val a, b = newTestActor(dispatcher.id)

      a ! Meet(aStart, aStop)
      assertCountDown(aStart, 3.seconds.dilated.toMillis, "Should process first message within 3 seconds")

      b ! CountDown(bParallel)
      assertCountDown(bParallel, 3.seconds.dilated.toMillis, "Should process other actors in parallel")

      aStop.countDown()

      system.stop(a)
      system.stop(b)

      while (!a.isTerminated && !b.isTerminated) {} //Busy wait for termination

      assertRefDefaultZero(a)(registers = 1, unregisters = 1, msgsReceived = 1, msgsProcessed = 1)
      assertRefDefaultZero(b)(registers = 1, unregisters = 1, msgsReceived = 1, msgsProcessed = 1)
    }
  }
}
