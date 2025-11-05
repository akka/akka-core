/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await

import akka.actor.{ Actor, Props }
import akka.pattern.ask
import akka.testkit.{ AkkaSpec, DefaultTimeout, ImplicitSender, TestLatch }

object BroadcastSpec {
  class TestActor extends Actor {
    def receive = { case _ => }
  }
}

class BroadcastSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {

  "broadcast group" must {

    "broadcast message using !" in {
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
      val routedActor = system.actorOf(BroadcastGroup(paths).props())
      routedActor ! 1
      routedActor ! "end"

      Await.ready(doneLatch, remainingOrDefault)

      counter1.get should ===(1)
      counter2.get should ===(1)
    }

    "broadcast message using ?" in {
      val doneLatch = new TestLatch(2)

      val counter1 = new AtomicInteger
      val actor1 = system.actorOf(Props(new Actor {
        def receive = {
          case "end" => doneLatch.countDown()
          case msg: Int =>
            counter1.addAndGet(msg)
            sender() ! "ack"
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
      val routedActor = system.actorOf(BroadcastGroup(paths).props())
      routedActor ? 1
      routedActor ! "end"

      Await.ready(doneLatch, remainingOrDefault)

      counter1.get should ===(1)
      counter2.get should ===(1)
    }
  }

}
