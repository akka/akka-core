/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }
import org.openjdk.jmh.annotations._

import akka.dispatch._
import akka.testkit.TestProbe
import akka.util.Helpers.ConfigOps

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SingleShotTime))
@Fork(5)
@Threads(1)
@Warmup(iterations = 10, batchSize = TellOnlyBenchmark.numMessages)
@Measurement(iterations = 10, batchSize = TellOnlyBenchmark.numMessages)
class TellOnlyBenchmark {
  import TellOnlyBenchmark._

  implicit var system: ActorSystem = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem(
      "TellOnlyBenchmark",
      ConfigFactory.parseString(s"""
          | akka {
          |   log-dead-letters = off
          |   actor {
          |     default-dispatcher {
          |       executor = "fork-join-executor"
          |       fork-join-executor {
          |         parallelism-min = 1
          |         parallelism-max = 4
          |       }
          |       throughput = 1
          |     }
          |   }
          | }
          | dropping-dispatcher {
          |   fork-join-executor.parallelism-min = 1
          |   fork-join-executor.parallelism-max = 1
          |   type = "akka.actor.TellOnlyBenchmark$$DroppingDispatcherConfigurator"
          |   mailbox-type = "akka.actor.TellOnlyBenchmark$$UnboundedDroppingMailbox"
          | }
          | """.stripMargin))
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)
  }

  var actor: ActorRef = _
  var probe: TestProbe = _

  @Setup(Level.Iteration)
  def setupIteration(): Unit = {
    actor = system.actorOf(Props[TellOnlyBenchmark.Echo]().withDispatcher("dropping-dispatcher"))
    probe = TestProbe()
    probe.watch(actor)
    probe.send(actor, message)
    probe.expectMsg(message)
    probe.send(actor, flipDrop)
    probe.expectNoMessage(200.millis)
    System.gc()
  }

  @TearDown(Level.Iteration)
  def shutdownIteration(): Unit = {
    probe.send(actor, flipDrop)
    probe.expectNoMessage(200.millis)
    actor ! stop
    probe.expectTerminated(actor, timeout)
    actor = null
    probe = null
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def tell(): Unit = {
    probe.send(actor, message)
  }
}

object TellOnlyBenchmark {
  final val stop = "stop"
  final val message = "message"
  final val flipDrop = "flipDrop"
  final val timeout = 5.seconds
  final val numMessages = 1000000

  class Echo extends Actor {
    def receive = {
      case `stop` =>
        context.stop(self)
      case m => sender() ! m
    }
  }

  class DroppingMessageQueue extends UnboundedMailbox.MessageQueue {
    @volatile var dropping = false

    override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      if (handle.message == flipDrop) dropping = !dropping
      else if (!dropping) super.enqueue(receiver, handle)
    }
  }

  case class UnboundedDroppingMailbox() extends MailboxType with ProducesMessageQueue[DroppingMessageQueue] {

    def this(settings: ActorSystem.Settings, config: Config) = this()

    final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
      new DroppingMessageQueue
  }

  class DroppingDispatcher(
      _configurator: MessageDispatcherConfigurator,
      _id: String,
      _throughput: Int,
      _throughputDeadlineTime: Duration,
      _executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
      _shutdownTimeout: FiniteDuration)
      extends Dispatcher(
        _configurator,
        _id,
        _throughput,
        _throughputDeadlineTime,
        _executorServiceFactoryProvider,
        _shutdownTimeout) {

    override protected[akka] def dispatch(receiver: ActorCell, invocation: Envelope): Unit = {
      val mbox = receiver.mailbox
      mbox.enqueue(receiver.self, invocation)
      mbox.messageQueue match {
        case mb: DroppingMessageQueue if mb.dropping => // do nothing
        case _                                       => registerForExecution(mbox, hasMessageHint = true, hasSystemMessageHint = false)
      }
    }
  }

  class DroppingDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends MessageDispatcherConfigurator(config, prerequisites) {

    override def dispatcher(): MessageDispatcher =
      new DroppingDispatcher(
        this,
        config.getString("id"),
        config.getInt("throughput"),
        config.getNanosDuration("throughput-deadline-time"),
        configureExecutor(),
        config.getMillisDuration("shutdown-timeout"))
  }
}
