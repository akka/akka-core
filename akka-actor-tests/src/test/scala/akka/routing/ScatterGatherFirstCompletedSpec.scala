/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ Actor, Props }
import akka.actor.ActorSystem
import akka.actor.Status
import akka.pattern.ask
import akka.testkit.{ AkkaSpec, DefaultTimeout, ImplicitSender, TestLatch }
import akka.testkit.TestProbe

object ScatterGatherFirstCompletedSpec {
  class TestActor extends Actor {
    def receive = { case _ => }
  }

  final case class Stop(id: Option[Int] = None)

  def newActor(id: Int, shudownLatch: Option[TestLatch] = None)(implicit system: ActorSystem) =
    system.actorOf(
      Props(new Actor {
        def receive = {
          case Stop(None)                     => context.stop(self)
          case Stop(Some(_id)) if (_id == id) => context.stop(self)
          case _id: Int if (_id == id)        =>
          case _ => {
            Thread.sleep(100 * id)
            sender() ! id
          }
        }

        override def postStop() = {
          shudownLatch.foreach(_.countDown())
        }
      }),
      "Actor:" + id)
}

class ScatterGatherFirstCompletedSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {
  import ScatterGatherFirstCompletedSpec._

  "Scatter-gather group" must {

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
      val routedActor = system.actorOf(ScatterGatherFirstCompletedGroup(paths, within = 1.second).props())
      routedActor ! Broadcast(1)
      routedActor ! Broadcast("end")

      Await.ready(doneLatch, TestLatch.DefaultTimeout)

      counter1.get should ===(1)
      counter2.get should ===(1)
    }

    "return response, even if one of the actors has stopped" in {
      val shutdownLatch = new TestLatch(1)
      val actor1 = newActor(1, Some(shutdownLatch))
      val actor2 = newActor(14, Some(shutdownLatch))
      val paths = List(actor1, actor2).map(_.path.toString)
      val routedActor = system.actorOf(ScatterGatherFirstCompletedGroup(paths, within = 3.seconds).props())

      routedActor ! Broadcast(Stop(Some(1)))
      Await.ready(shutdownLatch, TestLatch.DefaultTimeout)
      Await.result(routedActor ? Broadcast(0), timeout.duration) should ===(14)
    }

  }

  "Scatter-gather pool" must {

    "without routees should reply immediately" in {
      val probe = TestProbe()
      val router =
        system.actorOf(ScatterGatherFirstCompletedPool(nrOfInstances = 0, within = 5.seconds).props(Props.empty))
      router.tell("hello", probe.ref)
      probe.expectMsgType[Status.Failure](2.seconds).cause.getClass should be(classOf[TimeoutException])
    }

  }

}
