/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.actor.Status.Failure
import akka.pattern.{ ask, AskTimeoutException }
import akka.testkit._

object TailChoppingSpec {
  def newActor(id: Int, sleepTime: Duration)(implicit system: ActorSystem) =
    system.actorOf(Props(new Actor {
      var times: Int = _

      def receive = {
        case "stop"  => context.stop(self)
        case "times" => sender() ! times
        case _ =>
          times += 1
          Thread.sleep(sleepTime.toMillis)
          sender() ! "ack"
      }
    }), "Actor:" + id)
}

class TailChoppingSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {
  import TailChoppingSpec._

  def oneOfShouldEqual(what: Any, default: Any, ref: ActorRef*)(f: ActorRef => Any): Unit = {
    val results = ref.map(p => f(p))
    results.count(_ == what) should equal(1)
    results.count(_ == default) should equal(results.size - 1)
  }

  def allShouldEqual(what: Any, ref: ActorRef*)(f: ActorRef => Any): Unit = {
    val results = ref.map(p => f(p))
    results.count(_ == what) should equal(results.size)
  }

  "Tail-chopping group" must {

    "deliver a broadcast message using the !" in {
      val doneLatch = new TestLatch(2)

      val counter1 = new AtomicInteger
      val actor1 = system.actorOf(Props(new Actor {
        def receive = {
          case "end"    => doneLatch.countDown()
          case msg: Int => counter1.addAndGet(msg)
        }
      }))

      val counter2 = new AtomicInteger
      val actor2 = system.actorOf(Props(new Actor {
        def receive = {
          case "end"    => doneLatch.countDown()
          case msg: Int => counter2.addAndGet(msg)
        }
      }))

      val paths = List(actor1, actor2).map(_.path.toString)
      val routedActor = system.actorOf(TailChoppingGroup(paths, within = 1.second, interval = 100.millisecond).props())
      routedActor ! Broadcast(1)
      routedActor ! Broadcast("end")

      Await.ready(doneLatch, TestLatch.DefaultTimeout)

      counter1.get should ===(1)
      counter2.get should ===(1)
    }

    "return response from second actor after inactivity from first one" in {
      val actor1 = newActor(1, 1.millis)
      val actor2 = newActor(2, 1.millis)
      val probe = TestProbe()
      val paths = List(actor1, actor2).map(_.path.toString)
      val routedActor = system.actorOf(TailChoppingGroup(paths, within = 1.seconds, interval = 50.millisecond).props())

      probe.send(routedActor, "")
      probe.expectMsg("ack")

      oneOfShouldEqual(1, 1, actor1, actor2)(ref => Await.result(ref ? "times", timeout.duration))

      routedActor ! Broadcast("stop")
    }

    "throw exception if no result will arrive within given time" in {
      val actor1 = newActor(3, 500.millis)
      val actor2 = newActor(4, 500.millis)
      val probe = TestProbe()
      val paths = List(actor1, actor2).map(_.path.toString)
      val routedActor =
        system.actorOf(TailChoppingGroup(paths, within = 300.milliseconds, interval = 50.milliseconds).props())

      probe.send(routedActor, "")
      probe.expectMsgPF() {
        case Failure(_: AskTimeoutException) =>
      }

      allShouldEqual(1, actor1, actor2)(ref => Await.result(ref ? "times", timeout.duration))

      routedActor ! Broadcast("stop")
    }

    "reply ASAP" in {
      val actor1 = newActor(5, 1.seconds)
      val actor2 = newActor(6, 4.seconds)
      val probe = TestProbe()
      val paths = List(actor1, actor2).map(_.path.toString)
      val routedActor =
        system.actorOf(TailChoppingGroup(paths, within = 5.seconds, interval = 100.milliseconds).props())

      probe.send(routedActor, "")
      probe.expectMsg(max = 2.seconds, "ack")

      routedActor ! Broadcast("stop")
    }
  }
}
