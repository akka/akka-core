/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.{ BlockingQueue, ConcurrentLinkedQueue }

import scala.annotation.nowarn
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }
import language.postfixOps
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }

import akka.actor._
import akka.testkit.{ AkkaSpec, EventFilter }

abstract class MailboxSpec extends AkkaSpec with BeforeAndAfterAll with BeforeAndAfterEach {
  def name: String

  def factory: MailboxType => MessageQueue

  def supportsBeingBounded = true

  def maxConsumers = 4

  private val exampleMessage = createMessageInvocation("test")

  name should {

    "create an unbounded mailbox" in {
      val config = UnboundedMailbox()
      val q = factory(config)
      ensureInitialMailboxState(config, q)
    }

    "UnboundedMailbox.numberOfMessages should be consistent with queue size" in {
      ensureSingleConsumerEnqueueDequeue(UnboundedMailbox())
    }

    "BoundedMailbox.numberOfMessages should be consistent with queue size" in {
      ensureSingleConsumerEnqueueDequeue(BoundedMailbox(1000, 10 milliseconds))
    }

    "create a bounded mailbox with 10 capacity and with push timeout" in {
      val config = BoundedMailbox(10, 10 milliseconds)
      config.capacity should ===(10)
      val q = factory(config)
      ensureInitialMailboxState(config, q)

      for (_ <- 1 to config.capacity) q.enqueue(testActor, exampleMessage)

      q.numberOfMessages should ===(config.capacity)
      q.hasMessages should ===(true)

      system.eventStream.subscribe(testActor, classOf[DeadLetter])
      q.enqueue(testActor, exampleMessage)
      expectMsg(DeadLetter(exampleMessage.message, system.deadLetters, testActor))
      system.eventStream.unsubscribe(testActor, classOf[DeadLetter])

      q.dequeue() should ===(exampleMessage)
      q.numberOfMessages should ===(config.capacity - 1)
      q.hasMessages should ===(true)
    }

    "dequeue what was enqueued properly for unbounded mailboxes" in {
      testEnqueueDequeue(UnboundedMailbox())
    }

    "dequeue what was enqueued properly for bounded mailboxes" in {
      testEnqueueDequeue(BoundedMailbox(10000, -1 millisecond))
    }

    "dequeue what was enqueued properly for bounded mailboxes with 0 pushTimeout" in {
      testEnqueueDequeue(BoundedMailbox(10, 0 millisecond), 20, 10, false)
    }

    "dequeue what was enqueued properly for bounded mailboxes with pushTimeout" in {
      testEnqueueDequeue(BoundedMailbox(10000, 100 milliseconds))
    }
  }

  //CANDIDATE FOR TESTKIT
  def spawn[T <: AnyRef](fun: => T): Future[T] = Future(fun)(ExecutionContext.global)

  def createMessageInvocation(msg: Any): Envelope = Envelope(msg, system.deadLetters, system)

  def ensureMailboxSize(q: MessageQueue, expected: Int): Unit = q.numberOfMessages match {
    case -1 | `expected` =>
      q.hasMessages should ===(expected != 0)
    case other =>
      other should ===(expected)
      q.hasMessages should ===(expected != 0)
  }

  def ensureSingleConsumerEnqueueDequeue(config: MailboxType): Unit = {
    val q = factory(config)
    ensureMailboxSize(q, 0)
    q.dequeue() should ===(null)
    for (i <- 1 to 100) {
      q.enqueue(testActor, exampleMessage)
      ensureMailboxSize(q, i)
    }

    ensureMailboxSize(q, 100)

    for (i <- 99 to 0 by -1) {
      q.dequeue() should ===(exampleMessage)
      ensureMailboxSize(q, i)
    }

    q.dequeue() should ===(null)
    ensureMailboxSize(q, 0)
  }

  def ensureInitialMailboxState(config: MailboxType, q: MessageQueue): Unit = {
    q should not be null
    q match {
      case aQueue: BlockingQueue[_] =>
        config match {
          case BoundedMailbox(capacity, _) => aQueue.remainingCapacity should ===(capacity)
          case UnboundedMailbox()          => aQueue.remainingCapacity should ===(Int.MaxValue)
          case _                           => fail()
        }
      case _ =>
    }
    q.numberOfMessages should ===(0)
    q.hasMessages should ===(false)
  }

  def testEnqueueDequeue(
      config: MailboxType,
      enqueueN: Int = 10000,
      dequeueN: Int = 10000,
      parallel: Boolean = true): Unit = within(10 seconds) {
    val q = factory(config)
    ensureInitialMailboxState(config, q)

    EventFilter.warning(pattern = "received dead letter", occurrences = (enqueueN - dequeueN)).intercept {

      def createProducer(fromNum: Int, toNum: Int): Future[Vector[Envelope]] = spawn {
        val messages = Vector() ++ (for (i <- fromNum to toNum) yield createMessageInvocation(i))
        for (i <- messages) q.enqueue(testActor, i)
        messages
      }

      val producers = {
        val step = 500
        val ps = for (i <- (1 to enqueueN by step).toList) yield createProducer(i, Math.min(enqueueN, i + step - 1))

        if (parallel == false)
          ps.foreach { Await.ready(_, remainingOrDefault) }

        ps
      }

      def createConsumer: Future[Vector[Envelope]] = spawn {
        var r = Vector[Envelope]()

        while (producers.exists(_.isCompleted == false) || q.hasMessages) Option(q.dequeue()).foreach { message =>
          r = r :+ message
        }

        r
      }

      val consumers = List.fill(maxConsumers)(createConsumer)

      val ps = producers.map(Await.result(_, remainingOrDefault))
      val cs = consumers.map(Await.result(_, remainingOrDefault))

      ps.map(_.size).sum should ===(enqueueN) //Must have produced 1000 messages
      cs.map(_.size).sum should ===(dequeueN) //Must have consumed all produced messages
      //No message is allowed to be consumed by more than one consumer
      cs.flatten.distinct.size should ===(dequeueN)
      //All consumed messages should have been produced
      cs.flatten.diff(ps.flatten).size should ===(0)
      //The ones that were produced and not consumed
      ps.flatten.diff(cs.flatten).size should ===(enqueueN - dequeueN)
    }
  }
}

class DefaultMailboxSpec extends MailboxSpec {
  lazy val name = "The default mailbox implementation"
  def factory = {
    case u: UnboundedMailbox => u.create(None, None)
    case b: BoundedMailbox   => b.create(None, None)
    case _                   => throw new RuntimeException() // compiler exhaustiveness check pleaser
  }
}

class PriorityMailboxSpec extends MailboxSpec {
  val comparator = PriorityGenerator(_.##)
  lazy val name = "The priority mailbox implementation"
  def factory = {
    case UnboundedMailbox() => new UnboundedPriorityMailbox(comparator).create(None, None)
    case BoundedMailbox(capacity, pushTimeOut) =>
      new BoundedPriorityMailbox(comparator, capacity, pushTimeOut).create(None, None)
    case _ => throw new RuntimeException() // compiler exhaustiveness check pleaser
  }
}

class StablePriorityMailboxSpec extends MailboxSpec {
  val comparator = PriorityGenerator(_.##)
  lazy val name = "The stable priority mailbox implementation"
  def factory = {
    case UnboundedMailbox() => new UnboundedStablePriorityMailbox(comparator).create(None, None)
    case BoundedMailbox(capacity, pushTimeOut) =>
      new BoundedStablePriorityMailbox(comparator, capacity, pushTimeOut).create(None, None)
    case _ => throw new RuntimeException() // compiler exhaustiveness check pleaser
  }
}

class ControlAwareMailboxSpec extends MailboxSpec {
  lazy val name = "The control aware mailbox implementation"
  def factory = {
    case UnboundedMailbox() => new UnboundedControlAwareMailbox().create(None, None)
    case BoundedMailbox(capacity, pushTimeOut) =>
      new BoundedControlAwareMailbox(capacity, pushTimeOut).create(None, None)
    case _ => throw new RuntimeException() // compiler exhaustiveness check pleaser
  }
}

object CustomMailboxSpec {
  val config = """
    my-dispatcher {
       mailbox-type = "akka.dispatch.CustomMailboxSpec$MyMailboxType"
    }
    """

  class MyMailboxType(
      @nowarn("msg=never used") settings: ActorSystem.Settings,
      @nowarn("msg=never used") config: Config)
      extends MailboxType {
    override def create(owner: Option[ActorRef], system: Option[ActorSystem]) = owner match {
      case Some(o) => new MyMailbox(o)
      case None    => throw new Exception("no mailbox owner given")
    }
  }

  class MyMailbox(@nowarn("msg=never used") owner: ActorRef) extends UnboundedQueueBasedMessageQueue {
    final val queue = new ConcurrentLinkedQueue[Envelope]()
  }
}

class CustomMailboxSpec extends AkkaSpec(CustomMailboxSpec.config) {
  "Dispatcher configuration" must {
    "support custom mailboxType" in {
      val actor = system.actorOf(Props.empty.withDispatcher("my-dispatcher"))
      awaitCond(actor match {
        case r: RepointableRef => r.isStarted
        case _                 => true
      }, 1 second, 10 millis)
      val queue = actor.asInstanceOf[ActorRefWithCell].underlying.asInstanceOf[ActorCell].mailbox.messageQueue
      queue.getClass should ===(classOf[CustomMailboxSpec.MyMailbox])
    }
  }
}

class SingleConsumerOnlyMailboxSpec extends MailboxSpec {
  lazy val name = "The single-consumer-only mailbox implementation"
  override def maxConsumers = 1
  def factory = {
    case _: UnboundedMailbox            => SingleConsumerOnlyUnboundedMailbox().create(None, None)
    case _ @BoundedMailbox(capacity, _) => NonBlockingBoundedMailbox(capacity).create(None, None)
    case _                              => throw new RuntimeException() // compiler exhaustiveness check pleaser
  }
}

object SingleConsumerOnlyMailboxVerificationSpec {
  case object Ping
  val mailboxConf = ConfigFactory.parseString("""
      test-unbounded-dispatcher {
      mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
      throughput = 1
      }
      test-bounded-dispatcher {
      mailbox-type = "akka.dispatch.NonBlockingBoundedMailbox"
      mailbox-capacity = 1
      throughput = 1
      }""")
}

class SingleConsumerOnlyMailboxVerificationSpec
    extends AkkaSpec(SingleConsumerOnlyMailboxVerificationSpec.mailboxConf) {
  import SingleConsumerOnlyMailboxVerificationSpec.Ping

  def pathologicalPingPong(dispatcherId: String): Unit = {
    val total = 2000000
    val runner = system.actorOf(Props(new Actor {
      val a, b = context.watch(context.actorOf(Props(new Actor {
        var n = total / 2
        def receive = {
          case Ping =>
            n -= 1
            sender() ! Ping
            if (n == 0)
              context.stop(self)
        }
      }).withDispatcher(dispatcherId)))
      def receive = {
        case Ping                  => a.tell(Ping, b)
        case Terminated(`a` | `b`) => if (context.children.isEmpty) context.stop(self)
      }
    }))
    watch(runner)
    runner ! Ping
    expectTerminated(runner)
  }

  "A SingleConsumerOnlyMailbox" should {
    "support pathological ping-ponging for the unbounded case" in within(30.seconds) {
      pathologicalPingPong("test-unbounded-dispatcher")
    }

    "support pathological ping-ponging for the bounded case" in within(30.seconds) {
      pathologicalPingPong("test-bounded-dispatcher")
    }
  }
}
