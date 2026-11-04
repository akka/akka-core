/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.{ Collections, IdentityHashMap }
import java.util.concurrent.{ CountDownLatch, ExecutorService, ForkJoinPool, ThreadFactory }
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }

object MailboxSchedulingSpec {
  val config = """
    recording-dispatcher {
      executor = "akka.dispatch.MailboxSchedulingSpec$RecordingPoolConfigurator"
      throughput = 1
    }
    jdk-pool-dispatcher {
      executor = "akka.dispatch.MailboxSchedulingSpec$JdkPoolConfigurator"
    }
    affinity-dispatcher {
      executor = "affinity-pool-executor"
      affinity-pool-executor {
        parallelism-min = 4
        parallelism-max = 4
      }
    }
    """

  val submitted = new AtomicInteger
  val resubmitted = new AtomicInteger

  // re-submitting the same task instance can corrupt the FJP work queue, issue #33019
  final class RecordingPool extends ForkJoinPool(4, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true) {
    private val seen = Collections.newSetFromMap(new IdentityHashMap[Runnable, java.lang.Boolean])
    override def execute(task: Runnable): Unit = {
      val firstTime = seen.synchronized(seen.add(task))
      submitted.incrementAndGet()
      if (!firstTime) resubmitted.incrementAndGet()
      super.execute(task)
    }
  }

  class RecordingPoolConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends ExecutorServiceConfigurator(config, prerequisites) {
    def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
      new ExecutorServiceFactory {
        def createExecutorService: ExecutorService = new RecordingPool
      }
  }

  class JdkPoolConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends ExecutorServiceConfigurator(config, prerequisites) {
    def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
      new ExecutorServiceFactory {
        def createExecutorService: ExecutorService =
          new ForkJoinPool(1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true)
      }
  }

  final case class Ping(n: Int)
  final case class Countdown(n: Int, replyTo: ActorRef)

  class PingPong extends Actor {
    def receive = {
      case Ping(0)               => context.parent ! "done"
      case Ping(n)               => sender() ! Ping(n - 1)
      case Countdown(0, replyTo) => replyTo ! "done"
      case Countdown(n, replyTo) => self ! Countdown(n - 1, replyTo)
    }
  }

  class Parent(replyTo: ActorRef) extends Actor {
    private val a = context.actorOf(Props[PingPong]().withDispatcher("recording-dispatcher"))
    private val b = context.actorOf(Props[PingPong]().withDispatcher("recording-dispatcher"))
    def receive = {
      case "start" => a.tell(Ping(1000), b)
      case "done"  => replyTo ! "done"
    }
  }

  class InterruptingActor(latch: CountDownLatch) extends Actor {
    def receive = {
      case "interrupt" =>
        latch.await()
        Thread.currentThread().interrupt()
      case "ping" => sender() ! "pong"
    }
  }

  class ThreadNameActor extends Actor {
    def receive = {
      case _ => sender() ! Thread.currentThread().getName
    }
  }
}

class MailboxSchedulingSpec extends AkkaSpec(MailboxSchedulingSpec.config) with ImplicitSender {
  import MailboxSchedulingSpec._

  "Mailbox scheduling" must {

    "never submit the same task instance to the executor twice" in {
      submitted.set(0)
      resubmitted.set(0)

      // cross-sends
      system.actorOf(Props(new Parent(testActor)).withDispatcher("recording-dispatcher")) ! "start"
      expectMsg("done")

      // self-sends
      val selfSender = system.actorOf(Props[PingPong]().withDispatcher("recording-dispatcher"))
      selfSender ! Countdown(1000, testActor)
      expectMsg("done")

      // system messages
      val probe = TestProbe()
      (1 to 20).foreach { _ =>
        val ref = system.actorOf(Props[PingPong]().withDispatcher("recording-dispatcher"))
        probe.watch(ref)
        system.stop(ref)
        probe.expectTerminated(ref)
      }

      submitted.get() should be > 1000
      resubmitted.get() should ===(0)
    }

    "keep running an actor that was interrupted with messages pending on a plain JDK ForkJoinPool" in {
      val latch = new CountDownLatch(1)
      val ref = system.actorOf(Props(new InterruptingActor(latch)).withDispatcher("jdk-pool-dispatcher"))
      ref ! "interrupt"
      ref ! "ping"
      latch.countDown()
      expectMsg("pong")
    }

    "keep running a mailbox after a fatal error so that the actor system can terminate" in {
      val sys = ActorSystem(
        "MailboxSchedulingSpec-fatal",
        ConfigFactory.parseString("akka.jvm-exit-on-fatal-error = off").withFallback(system.settings.config))
      try {
        val ref = sys.actorOf(Props(new Actor {
          def receive = {
            case _ => throw new LinkageError("Simulated fatal error")
          }
        }))
        ref ! "boom"
        // the fatal error triggers termination, which needs the actor to process its termination
        Await.ready(sys.whenTerminated, 10.seconds)
      } finally shutdown(sys)
    }

    "keep running an actor on the same thread with the affinity pool" in {
      val ref = system.actorOf(Props[ThreadNameActor]().withDispatcher("affinity-dispatcher"))
      val threadNames = (1 to 300).map { _ =>
        ref ! "name"
        expectMsgType[String]
      }.toSet
      threadNames should have size 1
    }
  }
}
