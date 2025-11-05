/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

import language.postfixOps
import org.scalatest.BeforeAndAfterEach

import akka.actor._
import akka.dispatch.Dispatcher
import akka.event.Logging.Warning
import akka.pattern.ask

/**
 * Test whether TestActorRef behaves as an ActorRef should, besides its own spec.
 */
object TestActorRefSpec {

  var counter = 4
  val thread = Thread.currentThread
  var otherthread: Thread = null

  trait TActor extends Actor {
    def receive = new Receive {
      val recv = receiveT
      def isDefinedAt(o: Any) = recv.isDefinedAt(o)
      def apply(o: Any): Unit = {
        if (Thread.currentThread ne thread)
          otherthread = Thread.currentThread
        recv(o)
      }
    }
    def receiveT: Receive
  }

  class ReplyActor extends TActor {
    import context.system
    var replyTo: ActorRef = null

    def receiveT = {
      case "complexRequest" => {
        replyTo = sender()
        val worker = TestActorRef(Props[WorkerActor]())
        worker ! "work"
      }
      case "complexRequest2" =>
        val worker = TestActorRef(Props[WorkerActor]())
        worker ! sender()
      case "workDone"      => replyTo ! "complexReply"
      case "simpleRequest" => sender() ! "simpleReply"
    }
  }

  class WorkerActor() extends TActor {
    def receiveT = {
      case "work" =>
        sender() ! "workDone"
        context.stop(self)
      case replyTo: Promise[_] => replyTo.asInstanceOf[Promise[Any]].success("complexReply")
      case replyTo: ActorRef   => replyTo ! "complexReply"
    }

    val supervisor = context.parent
    val name = context.self.path.name
  }

  class SenderActor(replyActor: ActorRef) extends TActor {

    def receiveT = {
      case "complex"  => replyActor ! "complexRequest"
      case "complex2" => replyActor ! "complexRequest2"
      case "simple"   => replyActor ! "simpleRequest"
      case "complexReply" => {
        counter -= 1
      }
      case "simpleReply" => {
        counter -= 1
      }
    }
  }

  class Logger extends Actor {
    var count = 0
    var msg: String = _
    def receive = {
      case Warning(_, _, m: String) => count += 1; msg = m
    }
  }

  class ReceiveTimeoutActor(target: ActorRef) extends Actor {
    context.setReceiveTimeout(1.second)
    def receive = {
      case ReceiveTimeout =>
        target ! "timeout"
        context.stop(self)
    }
  }

  /**
   * Forwarding `Terminated` to non-watching testActor is not possible,
   * and therefore the `Terminated` message is wrapped.
   */
  final case class WrappedTerminated(t: Terminated)

}

class TestActorRefSpec extends AkkaSpec("disp1.type=Dispatcher") with BeforeAndAfterEach with DefaultTimeout {

  import TestActorRefSpec._

  override def beforeEach(): Unit = otherthread = null

  private def assertThread(): Unit = otherthread should (be(null).or(equal(thread)))

  "A TestActorRef should be an ActorRef, hence it" must {

    "support nested Actor creation" when {

      "used with TestActorRef" in {
        val a = TestActorRef(Props(new Actor {
          val nested = TestActorRef(Props(new Actor { def receive = { case _ => } }))
          def receive = { case _ => sender() ! nested }
        }))
        a should not be (null)
        val nested = Await.result((a ? "any").mapTo[ActorRef], timeout.duration)
        nested should not be (null)
        a should not be theSameInstanceAs(nested)
      }

      "used with ActorRef" in {
        val a = TestActorRef(Props(new Actor {
          val nested = context.actorOf(Props(new Actor { def receive = { case _ => } }))
          def receive = { case _ => sender() ! nested }
        }))
        a should not be (null)
        val nested = Await.result((a ? "any").mapTo[ActorRef], timeout.duration)
        nested should not be (null)
        a should not be theSameInstanceAs(nested)
      }

    }

    "support reply via sender()" in {
      val serverRef = TestActorRef(Props[ReplyActor]())
      val clientRef = TestActorRef(Props(classOf[SenderActor], serverRef))

      counter = 4

      clientRef ! "complex"
      clientRef ! "simple"
      clientRef ! "simple"
      clientRef ! "simple"

      counter should ===(0)

      counter = 4

      clientRef ! "complex2"
      clientRef ! "simple"
      clientRef ! "simple"
      clientRef ! "simple"

      counter should ===(0)

      assertThread()
    }

    "stop when sent a poison pill" in {
      EventFilter[ActorKilledException]().intercept {
        val a = TestActorRef(Props[WorkerActor]())
        system.actorOf(Props(new Actor {
          context.watch(a)
          def receive = {
            case t: Terminated => testActor.forward(WrappedTerminated(t))
            case x             => testActor.forward(x)
          }
        }))
        a.!(PoisonPill)(testActor)
        expectMsgPF(5 seconds) {
          case WrappedTerminated(Terminated(`a`)) => true
        }
        a.isTerminated should ===(true)
        assertThread()
      }
    }

    "restart when Kill:ed" in {
      EventFilter[ActorKilledException]().intercept {
        counter = 2

        val boss = TestActorRef(Props(new TActor {
          val ref = TestActorRef(Props(new TActor {
            def receiveT = { case _ => }
            override def preRestart(reason: Throwable, msg: Option[Any]): Unit = { counter -= 1 }
            override def postRestart(reason: Throwable): Unit = { counter -= 1 }
          }), self, "child")

          override def supervisorStrategy =
            OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 1 second)(List(classOf[ActorKilledException]))

          def receiveT = { case "sendKill" => ref ! Kill }
        }))

        boss ! "sendKill"

        counter should ===(0)
        assertThread()
      }
    }

    "support futures" in {
      val a = TestActorRef[WorkerActor]
      val f = a ? "work"
      // CallingThreadDispatcher means that there is no delay
      f.isCompleted should be(true)
      Await.result(f, timeout.duration) should ===("workDone")
    }

    "support receive timeout" in {
      TestActorRef(new ReceiveTimeoutActor(testActor))
      expectMsg("timeout")
    }

  }

  "A TestActorRef" must {

    "allow access to internals" in {
      class TA extends TActor {
        var s: String = _
        def receiveT = {
          case x: String => s = x
        }
      }
      val ref = TestActorRef(new TA)
      ref ! "hallo"
      val actor = ref.underlyingActor
      actor.s should ===("hallo")
    }

    "set receiveTimeout to None" in {
      val a = TestActorRef[WorkerActor]
      (a.underlyingActor.context.receiveTimeout should be).theSameInstanceAs(Duration.Undefined)
    }

    "set CallingThreadDispatcher" in {
      val a = TestActorRef[WorkerActor]
      a.underlying.dispatcher.getClass should ===(classOf[CallingThreadDispatcher])
    }

    "allow override of dispatcher" in {
      val a = TestActorRef(Props[WorkerActor]().withDispatcher("disp1"))
      a.underlying.dispatcher.getClass should ===(classOf[Dispatcher])
    }

    "proxy receive for the underlying actor without sender()" in {
      val ref = TestActorRef[WorkerActor]
      ref.receive("work")
      ref.isTerminated should ===(true)
    }

    "proxy receive for the underlying actor with sender()" in {
      val ref = TestActorRef[WorkerActor]
      ref.receive("work", testActor)
      ref.isTerminated should ===(true)
      expectMsg("workDone")
    }

    "not throw an exception when parent is passed in the apply" in {
      EventFilter[RuntimeException](occurrences = 1, message = "expected").intercept {
        val parent = TestProbe()
        val child = TestActorRef(Props(new Actor {
          def receive: Receive = {
            case 1 => throw new RuntimeException("expected")
            case x => sender() ! x
          }
        }), parent.ref, "Child")

        child ! 1
      }
    }
    "not throw an exception when child is created through childActorOf" in {
      EventFilter[RuntimeException](occurrences = 1, message = "expected").intercept {
        val parent = TestProbe()
        val child = parent.childActorOf(Props(new Actor {
          def receive: Receive = {
            case 1 => throw new RuntimeException("expected")
            case x => sender() ! x
          }
        }), "Child")

        child ! 1
      }
    }

  }

  "A TestActorRef Companion Object" must {

    "allow creation of a TestActorRef with a default supervisor" in {
      val ref = TestActorRef[WorkerActor]
      ref.underlyingActor.supervisor should be(system.asInstanceOf[ActorSystemImpl].guardian)
    }

    "allow creation of a TestActorRef with a default supervisor and specified name" in {
      val ref = TestActorRef[WorkerActor]("specificActor")
      ref.underlyingActor.name should be("specificActor")
    }

    "allow creation of a TestActorRef with a specified supervisor" in {
      val parent = TestActorRef[ReplyActor]
      val ref = TestActorRef[WorkerActor](parent)
      ref.underlyingActor.supervisor should be(parent)
    }

    "allow creation of a TestActorRef with a specified supervisor and specified name" in {
      val parent = TestActorRef[ReplyActor]
      val ref = TestActorRef[WorkerActor](parent, "specificSupervisedActor")
      ref.underlyingActor.name should be("specificSupervisedActor")
      ref.underlyingActor.supervisor should be(parent)
    }

    "allow creation of a TestActorRef with a default supervisor with Props" in {
      val ref = TestActorRef[WorkerActor](Props[WorkerActor]())
      ref.underlyingActor.supervisor should be(system.asInstanceOf[ActorSystemImpl].guardian)
    }

    "allow creation of a TestActorRef with a default supervisor and specified name with Props" in {
      val ref = TestActorRef[WorkerActor](Props[WorkerActor](), "specificPropsActor")
      ref.underlyingActor.name should be("specificPropsActor")
    }

    "allow creation of a TestActorRef with a specified supervisor with Props" in {
      val parent = TestActorRef[ReplyActor]
      val ref = TestActorRef[WorkerActor](Props[WorkerActor](), parent)
      ref.underlyingActor.supervisor should be(parent)
    }

    "allow creation of a TestActorRef with a specified supervisor and specified name with Props" in {
      val parent = TestActorRef[ReplyActor]
      val ref = TestActorRef[WorkerActor](Props[WorkerActor](), parent, "specificSupervisedPropsActor")
      ref.underlyingActor.name should be("specificSupervisedPropsActor")
      ref.underlyingActor.supervisor should be(parent)
    }

  }
}
