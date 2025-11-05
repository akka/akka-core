/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import language.postfixOps

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated
import akka.pattern.ask
import akka.testkit._

class RoundRobinSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {

  def routeeSize(router: ActorRef): Int =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees.size

  "round robin pool" must {

    "be able to shut down its instance" in {
      val helloLatch = new TestLatch(5)
      val stopLatch = new TestLatch(5)

      val actor = system.actorOf(RoundRobinPool(5).props(routeeProps = Props(new Actor {
        def receive = {
          case "hello" => helloLatch.countDown()
        }

        override def postStop(): Unit = {
          stopLatch.countDown()
        }
      })), "round-robin-shutdown")

      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      Await.ready(helloLatch, 5 seconds)

      system.stop(actor)
      Await.ready(stopLatch, 5 seconds)
    }

    "deliver messages in a round robin fashion" in {
      val connectionCount = 10
      val iterationCount = 10
      val doneLatch = new TestLatch(connectionCount)

      val counter = new AtomicInteger
      var replies: Map[Int, Int] = Map.empty.withDefaultValue(0)

      val actor = system.actorOf(RoundRobinPool(connectionCount).props(routeeProps = Props(new Actor {
        lazy val id = counter.getAndIncrement()
        def receive = {
          case "hit" => sender() ! id
          case "end" => doneLatch.countDown()
        }
      })), "round-robin")

      for (_ <- 1 to iterationCount; _ <- 1 to connectionCount) {
        val id = Await.result((actor ? "hit").mapTo[Int], timeout.duration)
        replies += (id -> (replies(id) + 1))
      }

      counter.get should ===(connectionCount)

      actor ! akka.routing.Broadcast("end")
      Await.ready(doneLatch, 5 seconds)

      replies.values.foreach { _ should ===(iterationCount) }
    }

    "deliver a broadcast message using the !" in {
      val helloLatch = new TestLatch(5)
      val stopLatch = new TestLatch(5)

      val actor = system.actorOf(RoundRobinPool(5).props(routeeProps = Props(new Actor {
        def receive = {
          case "hello" => helloLatch.countDown()
        }

        override def postStop(): Unit = {
          stopLatch.countDown()
        }
      })), "round-robin-broadcast")

      actor ! akka.routing.Broadcast("hello")
      Await.ready(helloLatch, 5 seconds)

      system.stop(actor)
      Await.ready(stopLatch, 5 seconds)
    }

    "be controlled with management messages" in {
      val actor = system.actorOf(RoundRobinPool(3).props(routeeProps = Props(new Actor {
        def receive = Actor.emptyBehavior
      })), "round-robin-managed")

      routeeSize(actor) should ===(3)
      actor ! AdjustPoolSize(+4)
      routeeSize(actor) should ===(7)
      actor ! AdjustPoolSize(-2)
      routeeSize(actor) should ===(5)

      val other = ActorSelectionRoutee(system.actorSelection("/user/other"))
      actor ! AddRoutee(other)
      routeeSize(actor) should ===(6)
      actor ! RemoveRoutee(other)
      routeeSize(actor) should ===(5)
    }
  }

  "round robin group" must {

    "deliver messages in a round robin fashion" in {
      val connectionCount = 10
      val iterationCount = 10
      val doneLatch = new TestLatch(connectionCount)

      var replies: Map[String, Int] = Map.empty.withDefaultValue(0)

      val paths = (1 to connectionCount).map { n =>
        val ref = system.actorOf(Props(new Actor {
          def receive = {
            case "hit" => sender() ! self.path.name
            case "end" => doneLatch.countDown()
          }
        }), name = "target-" + n)
        ref.path.toStringWithoutAddress
      }

      val actor = system.actorOf(RoundRobinGroup(paths).props(), "round-robin-group1")

      for (_ <- 1 to iterationCount; _ <- 1 to connectionCount) {
        val id = Await.result((actor ? "hit").mapTo[String], timeout.duration)
        replies += (id -> (replies(id) + 1))
      }

      actor ! akka.routing.Broadcast("end")
      Await.ready(doneLatch, 5 seconds)

      replies.values.foreach { _ should ===(iterationCount) }
    }
  }

  "round robin logic used in actor" must {
    "deliver messages in a round robin fashion" in {
      val connectionCount = 10
      val iterationCount = 10

      var replies: Map[String, Int] = Map.empty.withDefaultValue(0)

      val actor = system.actorOf(Props(new Actor {
        var n = 0
        var router = Router(RoundRobinRoutingLogic())

        def receive = {
          case p: Props =>
            n += 1
            val c = context.actorOf(p, name = "child-" + n)
            context.watch(c)
            router = router.addRoutee(c)
          case Terminated(c) =>
            router = router.removeRoutee(c)
            if (router.routees.isEmpty)
              context.stop(self)
          case other => router.route(other, sender())
        }
      }))

      val childProps = Props(new Actor {
        def receive = {
          case "hit" => sender() ! self.path.name
          case "end" => context.stop(self)
        }
      })

      (1 to connectionCount).foreach { _ =>
        actor ! childProps
      }

      for (_ <- 1 to iterationCount; _ <- 1 to connectionCount) {
        val id = Await.result((actor ? "hit").mapTo[String], timeout.duration)
        replies += (id -> (replies(id) + 1))
      }

      watch(actor)
      actor ! akka.routing.Broadcast("end")
      expectTerminated(actor)

      replies.values.foreach { _ should ===(iterationCount) }
    }
  }

}
