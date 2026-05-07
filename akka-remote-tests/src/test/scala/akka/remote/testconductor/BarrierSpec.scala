/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testconductor

import java.net.{ InetAddress, InetSocketAddress }

import scala.concurrent.duration._

import language.postfixOps

import akka.actor._
import akka.testkit.{ AkkaSpec, EventFilter, ImplicitSender, TestProbe, TimingTest }

object BarrierSpec {
  final case class Failed(ref: ActorRef, thr: Throwable)
  val config = """
    akka.testconductor.barrier-timeout = 5s
    akka.actor.provider = remote
    akka.actor.debug.fsm = on
    akka.actor.debug.lifecycle = on
               """
}

class BarrierSpec extends AkkaSpec(BarrierSpec.config) with ImplicitSender {

  import BarrierCoordinator._
  import BarrierSpec._
  import Controller._

  val A = RoleName("a")
  val B = RoleName("b")
  val C = RoleName("c")

  "A BarrierCoordinator" must {

    "register clients and remove them" taggedAs TimingTest in {
      val b = getBarrier()
      b ! NodeInfo(A, AddressFromURIString("akka://sys"), system.deadLetters)
      b ! RemoveClient(B)
      b ! RemoveClient(A)
      EventFilter[BarrierEmpty](occurrences = 1).intercept {
        b ! RemoveClient(A)
      }
      expectMsg(Failed(b, BarrierEmpty(Data(Set(), "", Nil, null), "cannot remove RoleName(a): no client to remove")))
    }

    "register clients and disconnect them" taggedAs TimingTest in {
      val b = getBarrier()
      b ! NodeInfo(A, AddressFromURIString("akka://sys"), system.deadLetters)
      b ! ClientDisconnected(B)
      expectNoMessage(1 second)
      b ! ClientDisconnected(A)
      expectNoMessage(1 second)
    }

    "fail entering barrier when nobody registered" taggedAs TimingTest in {
      val b = getBarrier()
      b ! EnterBarrier("bar1", None)
      expectMsg(ToClient(BarrierResult("bar1", false)))
    }

    "enter barrier" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b = TestProbe()
      barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      a.send(barrier, EnterBarrier("bar2", None))
      noMsg(a, b)
      within(2 seconds) {
        b.send(barrier, EnterBarrier("bar2", None))
        a.expectMsg(ToClient(BarrierResult("bar2", true)))
        b.expectMsg(ToClient(BarrierResult("bar2", true)))
      }
    }

    "enter barrier with joining node" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b, c = TestProbe()
      barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      a.send(barrier, EnterBarrier("bar3", None))
      barrier ! NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
      b.send(barrier, EnterBarrier("bar3", None))
      noMsg(a, b, c)
      within(2 seconds) {
        c.send(barrier, EnterBarrier("bar3", None))
        a.expectMsg(ToClient(BarrierResult("bar3", true)))
        b.expectMsg(ToClient(BarrierResult("bar3", true)))
        c.expectMsg(ToClient(BarrierResult("bar3", true)))
      }
    }

    "enter barrier with leaving node" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b, c = TestProbe()
      barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      barrier ! NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
      a.send(barrier, EnterBarrier("bar4", None))
      b.send(barrier, EnterBarrier("bar4", None))
      barrier ! RemoveClient(A)
      barrier ! ClientDisconnected(A)
      noMsg(a, b, c)
      b.within(2 seconds) {
        barrier ! RemoveClient(C)
        b.expectMsg(ToClient(BarrierResult("bar4", true)))
      }
      barrier ! ClientDisconnected(C)
      expectNoMessage(1 second)
    }

    "leave barrier when last “arrived” is removed" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b = TestProbe()
      barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      a.send(barrier, EnterBarrier("bar5", None))
      barrier ! RemoveClient(A)
      b.send(barrier, EnterBarrier("foo", None))
      b.expectMsg(ToClient(BarrierResult("foo", true)))
    }

    "fail barrier with disconnecing node" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b = TestProbe()
      val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      barrier ! nodeA
      barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      a.send(barrier, EnterBarrier("bar6", None))
      EventFilter[ClientLost](occurrences = 1).intercept {
        barrier ! ClientDisconnected(B)
      }
      val msg = expectMsgType[Failed]
      msg match {
        case Failed(_, thr: ClientLost)
            if thr == ClientLost(Data(Set(nodeA), "bar6", a.ref :: Nil, thr.data.deadline), B) =>
        case x =>
          fail(
            "Expected " + Failed(barrier, ClientLost(Data(Set(nodeA), "bar6", a.ref :: Nil, null), B)) + " but got " + x)
      }
    }

    "fail barrier with disconnecing node who already arrived" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b, c = TestProbe()
      val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      val nodeC = NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
      barrier ! nodeA
      barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      barrier ! nodeC
      a.send(barrier, EnterBarrier("bar7", None))
      b.send(barrier, EnterBarrier("bar7", None))
      EventFilter[ClientLost](occurrences = 1).intercept {
        barrier ! ClientDisconnected(B)
      }
      val msg = expectMsgType[Failed]
      msg match {
        case Failed(_, thr: ClientLost)
            if thr == ClientLost(Data(Set(nodeA, nodeC), "bar7", a.ref :: Nil, thr.data.deadline), B) =>
        case x =>
          fail(
            "Expected " + Failed(barrier, ClientLost(Data(Set(nodeA, nodeC), "bar7", a.ref :: Nil, null), B)) + " but got " + x)
      }
    }

    "fail when entering wrong barrier" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b = TestProbe()
      val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      barrier ! nodeA
      val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      barrier ! nodeB
      a.send(barrier, EnterBarrier("bar8", None))
      EventFilter[WrongBarrier](occurrences = 1).intercept {
        b.send(barrier, EnterBarrier("foo", None))
      }
      val msg = expectMsgType[Failed]
      msg match {
        case Failed(_, thr: WrongBarrier)
            if thr == WrongBarrier("foo", b.ref, Data(Set(nodeA, nodeB), "bar8", a.ref :: Nil, thr.data.deadline)) =>
        case x =>
          fail(
            "Expected " + Failed(
              barrier,
              WrongBarrier("foo", b.ref, Data(Set(nodeA, nodeB), "bar8", a.ref :: Nil, null))) + " but got " + x)
      }
    }

    "fail barrier after first failure" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a = TestProbe()
      EventFilter[BarrierEmpty](occurrences = 1).intercept {
        barrier ! RemoveClient(A)
      }
      val msg = expectMsgType[Failed]
      msg match {
        case Failed(_, thr: BarrierEmpty)
            if thr == BarrierEmpty(
                Data(Set(), "", Nil, thr.data.deadline),
                "cannot remove RoleName(a): no client to remove") =>
        case x =>
          fail("Expected " + Failed(
            barrier,
            BarrierEmpty(Data(Set(), "", Nil, null), "cannot remove RoleName(a): no client to remove")) + " but got " + x)
      }
      barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      a.send(barrier, EnterBarrier("bar9", None))
      a.expectMsg(ToClient(BarrierResult("bar9", false)))
    }

    "fail after barrier timeout" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b = TestProbe()
      val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
      barrier ! nodeA
      barrier ! nodeB
      a.send(barrier, EnterBarrier("bar10", None))
      EventFilter[BarrierTimeout](occurrences = 1).intercept {
        val msg = expectMsgType[Failed](7 seconds)
        msg match {
          case Failed(_, thr: BarrierTimeout)
              if thr == BarrierTimeout(Data(Set(nodeA, nodeB), "bar10", a.ref :: Nil, thr.data.deadline)) =>
          case x =>
            fail(
              "Expected " + Failed(barrier, BarrierTimeout(Data(Set(nodeA, nodeB), "bar10", a.ref :: Nil, null))) + " but got " + x)
        }
      }
    }

    "fail if a node registers twice" taggedAs TimingTest in {
      val barrier = getBarrier()
      val a, b = TestProbe()
      val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
      val nodeB = NodeInfo(A, AddressFromURIString("akka://sys"), b.ref)
      barrier ! nodeA
      EventFilter[DuplicateNode](occurrences = 1).intercept {
        barrier ! nodeB
      }
      val msg = expectMsgType[Failed]
      msg match {
        case Failed(_, thr: DuplicateNode)
            if thr == DuplicateNode(Data(Set(nodeA), "", Nil, thr.data.deadline), nodeB) =>
        case x =>
          fail("Expected " + Failed(barrier, DuplicateNode(Data(Set(nodeA), "", Nil, null), nodeB)) + " but got " + x)
      }
    }

    "finally have no failure messages left" taggedAs TimingTest in {
      expectNoMessage(1 second)
    }

  }

  "A Controller with BarrierCoordinator" must {

    "register clients and remove them" taggedAs TimingTest in {
      withController(1) { b =>
        b ! NodeInfo(A, AddressFromURIString("akka://sys"), testActor)
        expectMsg(ToClient(Done))
        b ! Remove(B)
        b ! Remove(A)
        EventFilter.warning(start = "cannot remove", occurrences = 1).intercept {
          b ! Remove(A)
        }
        Thread.sleep(5000)
      }
    }

    "register clients and disconnect them" taggedAs TimingTest in {
      withController(1) { b =>
        b ! NodeInfo(A, AddressFromURIString("akka://sys"), testActor)
        expectMsg(ToClient(Done))
        b ! ClientDisconnected(B)
        expectNoMessage(1 second)
        b ! ClientDisconnected(A)
        expectNoMessage(1 second)
      }
    }

    "fail entering barrier when nobody registered" taggedAs TimingTest in {
      withController(0) { b =>
        b ! EnterBarrier("b", None)
        expectMsg(ToClient(BarrierResult("b", false)))
      }
    }

    "enter barrier" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b = TestProbe()
        barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar11", None))
        noMsg(a, b)
        within(2 seconds) {
          b.send(barrier, EnterBarrier("bar11", None))
          a.expectMsg(ToClient(BarrierResult("bar11", true)))
          b.expectMsg(ToClient(BarrierResult("bar11", true)))
        }
      }
    }

    "enter barrier with joining node" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b, c = TestProbe()
        barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar12", None))
        barrier ! NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
        c.expectMsg(ToClient(Done))
        b.send(barrier, EnterBarrier("bar12", None))
        noMsg(a, b, c)
        within(2 seconds) {
          c.send(barrier, EnterBarrier("bar12", None))
          a.expectMsg(ToClient(BarrierResult("bar12", true)))
          b.expectMsg(ToClient(BarrierResult("bar12", true)))
          c.expectMsg(ToClient(BarrierResult("bar12", true)))
        }
      }
    }

    "enter barrier with leaving node" taggedAs TimingTest in {
      withController(3) { barrier =>
        val a, b, c = TestProbe()
        barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        barrier ! NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        c.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar13", None))
        b.send(barrier, EnterBarrier("bar13", None))
        barrier ! Remove(A)
        barrier ! ClientDisconnected(A)
        noMsg(a, b, c)
        b.within(2 seconds) {
          barrier ! Remove(C)
          b.expectMsg(ToClient(BarrierResult("bar13", true)))
        }
        barrier ! ClientDisconnected(C)
        expectNoMessage(1 second)
      }
    }

    "leave barrier when last “arrived” is removed" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b = TestProbe()
        barrier ! NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar14", None))
        barrier ! Remove(A)
        b.send(barrier, EnterBarrier("foo", None))
        b.expectMsg(ToClient(BarrierResult("foo", true)))
      }
    }

    "fail barrier with disconnecing node" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        barrier ! nodeA
        barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar15", None))
        barrier ! ClientDisconnected(RoleName("unknown"))
        noMsg(a)
        EventFilter[ClientLost](occurrences = 1).intercept {
          barrier ! ClientDisconnected(B)
        }
        a.expectMsg(ToClient(BarrierResult("bar15", false)))
      }
    }

    "fail barrier with disconnecing node who already arrived" taggedAs TimingTest in {
      withController(3) { barrier =>
        val a, b, c = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeC = NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
        barrier ! nodeA
        barrier ! NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        barrier ! nodeC
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        c.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar16", None))
        b.send(barrier, EnterBarrier("bar16", None))
        EventFilter[ClientLost](occurrences = 1).intercept {
          barrier ! ClientDisconnected(B)
        }
        a.expectMsg(ToClient(BarrierResult("bar16", false)))
      }
    }

    "fail when entering wrong barrier" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        barrier ! nodeA
        val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        barrier ! nodeB
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar17", None))
        EventFilter[WrongBarrier](occurrences = 1).intercept {
          b.send(barrier, EnterBarrier("foo", None))
        }
        a.expectMsg(ToClient(BarrierResult("bar17", false)))
        b.expectMsg(ToClient(BarrierResult("foo", false)))
      }
    }

    "fail after barrier timeout" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        barrier ! nodeA
        barrier ! nodeB
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar18", Option(2 seconds)))
        EventFilter[BarrierTimeout](occurrences = 1).intercept {
          Thread.sleep(4000)
        }
        b.send(barrier, EnterBarrier("bar18", None))
        a.expectMsg(ToClient(BarrierResult("bar18", false)))
        b.expectMsg(ToClient(BarrierResult("bar18", false)))
      }
    }

    "fail if a node registers twice" taggedAs TimingTest in {
      withController(2) { controller =>
        val a, b = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeB = NodeInfo(A, AddressFromURIString("akka://sys"), b.ref)
        controller ! nodeA
        EventFilter[DuplicateNode](occurrences = 1).intercept {
          controller ! nodeB
        }
        a.expectMsg(ToClient(BarrierResult("initial startup", false)))
        b.expectMsg(ToClient(BarrierResult("initial startup", false)))
      }
    }

    "fail subsequent barriers if a node registers twice" taggedAs TimingTest in {
      withController(1) { controller =>
        val a, b = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeB = NodeInfo(A, AddressFromURIString("akka://sys"), b.ref)
        controller ! nodeA
        a.expectMsg(ToClient(Done))
        EventFilter[DuplicateNode](occurrences = 1).intercept {
          controller ! nodeB
          b.expectMsg(ToClient(BarrierResult("initial startup", false)))
        }
        a.send(controller, EnterBarrier("bar19", None))
        a.expectMsg(ToClient(BarrierResult("bar19", false)))
      }
    }

    "fail subsequent barriers after foreced failure" taggedAs TimingTest in {
      withController(2) { barrier =>
        val a, b = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        barrier ! nodeA
        barrier ! nodeB
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar20", Option(2 seconds)))
        EventFilter[FailedBarrier](occurrences = 1).intercept {
          b.send(barrier, FailBarrier("bar20"))
          a.expectMsg(ToClient(BarrierResult("bar20", false)))
          b.expectNoMessage(1 second)
        }
        a.send(barrier, EnterBarrier("bar21", None))
        b.send(barrier, EnterBarrier("bar21", None))
        a.expectMsg(ToClient(BarrierResult("bar21", false)))
        b.expectMsg(ToClient(BarrierResult("bar21", false)))
      }
    }

    "timeout within the shortest timeout if the new timeout is shorter" taggedAs TimingTest in {
      withController(3) { barrier =>
        val a, b, c = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        val nodeC = NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
        barrier ! nodeA
        barrier ! nodeB
        barrier ! nodeC
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        c.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar22", Option(10 seconds)))
        b.send(barrier, EnterBarrier("bar22", Option(2 seconds)))
        EventFilter[BarrierTimeout](occurrences = 1).intercept {
          Thread.sleep(4000)
        }
        c.send(barrier, EnterBarrier("bar22", None))
        a.expectMsg(ToClient(BarrierResult("bar22", false)))
        b.expectMsg(ToClient(BarrierResult("bar22", false)))
        c.expectMsg(ToClient(BarrierResult("bar22", false)))
      }
    }

    "timeout within the shortest timeout if the new timeout is longer" taggedAs TimingTest in {
      withController(3) { barrier =>
        val a, b, c = TestProbe()
        val nodeA = NodeInfo(A, AddressFromURIString("akka://sys"), a.ref)
        val nodeB = NodeInfo(B, AddressFromURIString("akka://sys"), b.ref)
        val nodeC = NodeInfo(C, AddressFromURIString("akka://sys"), c.ref)
        barrier ! nodeA
        barrier ! nodeB
        barrier ! nodeC
        a.expectMsg(ToClient(Done))
        b.expectMsg(ToClient(Done))
        c.expectMsg(ToClient(Done))
        a.send(barrier, EnterBarrier("bar23", Option(2 seconds)))
        b.send(barrier, EnterBarrier("bar23", Option(10 seconds)))
        EventFilter[BarrierTimeout](occurrences = 1).intercept {
          Thread.sleep(4000)
        }
        c.send(barrier, EnterBarrier("bar23", None))
        a.expectMsg(ToClient(BarrierResult("bar23", false)))
        b.expectMsg(ToClient(BarrierResult("bar23", false)))
        c.expectMsg(ToClient(BarrierResult("bar23", false)))
      }
    }

    "finally have no failure messages left" taggedAs TimingTest in {
      expectNoMessage(1 second)
    }

  }

  private def withController(participants: Int)(f: ActorRef => Unit): Unit = {
    system.actorOf(Props(new Actor {
      val controller =
        context.actorOf(Props(classOf[Controller], participants, new InetSocketAddress(InetAddress.getLocalHost, 0)))
      controller ! GetSockAddr
      override def supervisorStrategy = OneForOneStrategy() {
        case x => testActor ! Failed(controller, x); SupervisorStrategy.Restart
      }
      def receive = {
        case _: InetSocketAddress => testActor ! controller
      }
    }).withDeploy(Deploy.local))
    val actor = expectMsgType[ActorRef]
    f(actor)
    actor ! PoisonPill // clean up so network connections don't accumulate during test run
  }

  /**
   * Produce a BarrierCoordinator which is supervised with a strategy which
   * forwards all failures to the testActor.
   */
  private def getBarrier(): ActorRef = {
    system.actorOf(Props(new Actor {
      val barrier = context.actorOf(Props[BarrierCoordinator]())
      override def supervisorStrategy = OneForOneStrategy() {
        case x => testActor ! Failed(barrier, x); SupervisorStrategy.Restart
      }
      def receive = {
        case _ => sender() ! barrier
      }
    }).withDeploy(Deploy.local)) ! ""
    expectMsgType[ActorRef]
  }

  private def noMsg(probes: TestProbe*): Unit = {
    expectNoMessage(1 second)
    probes.foreach(_.msgAvailable should ===(false))
  }

}
