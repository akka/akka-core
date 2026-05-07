/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.Config
import language.postfixOps

import akka.actor.{ Actor, ActorSystem, Props }
import akka.testkit.{ AkkaSpec, DefaultTimeout }

object StablePriorityDispatcherSpec {
  case object Result

  val config = """
    unbounded-stable-prio-dispatcher {
      mailbox-type = "akka.dispatch.StablePriorityDispatcherSpec$Unbounded"
    }
    bounded-stable-prio-dispatcher {
      mailbox-type = "akka.dispatch.StablePriorityDispatcherSpec$Bounded"
    }
    """

  class Unbounded(@nowarn("msg=never used") settings: ActorSystem.Settings, @nowarn("msg=never used") config: Config)
      extends UnboundedStablePriorityMailbox(PriorityGenerator({
        case i: Int if i <= 100 => i // Small integers have high priority
        case _: Int             => 101 // Don't care for other integers
        case Result             => Int.MaxValue
        case _                  => throw new RuntimeException() // compiler exhaustiveness check pleaser
      }: Any => Int))

  class Bounded(@nowarn("msg=never used") settings: ActorSystem.Settings, @nowarn("msg=never used") config: Config)
      extends BoundedStablePriorityMailbox(PriorityGenerator({
        case i: Int if i <= 100 => i // Small integers have high priority
        case _: Int             => 101 // Don't care for other integers
        case Result             => Int.MaxValue
        case _                  => throw new RuntimeException() // compiler exhaustiveness check pleaser
      }: Any => Int), 1000, 10 seconds)

}

class StablePriorityDispatcherSpec extends AkkaSpec(StablePriorityDispatcherSpec.config) with DefaultTimeout {
  import StablePriorityDispatcherSpec._

  "A StablePriorityDispatcher" must {
    "Order its messages according to the specified comparator while preserving FIFO for equal priority messages, " +
    "using an unbounded mailbox" in {
      val dispatcherKey = "unbounded-stable-prio-dispatcher"
      testOrdering(dispatcherKey)
    }

    "Order its messages according to the specified comparator while preserving FIFO for equal priority messages, " +
    "using a bounded mailbox" in {
      val dispatcherKey = "bounded-stable-prio-dispatcher"
      testOrdering(dispatcherKey)
    }

    def testOrdering(dispatcherKey: String): Unit = {
      val msgs = (1 to 200) toList
      val shuffled = scala.util.Random.shuffle(msgs)

      // It's important that the actor under test is not a top level actor
      // with RepointableActorRef, since messages might be queued in
      // UnstartedCell and then sent to the StablePriorityQueue and consumed immediately
      // without the ordering taking place.
      system.actorOf(Props(new Actor {
        context.actorOf(Props(new Actor {

          val acc = scala.collection.mutable.ListBuffer[Int]()

          shuffled.foreach { m =>
            self ! m
          }

          self.tell(Result, testActor)

          def receive = {
            case i: Int => acc += i
            case Result => sender() ! acc.toList
          }
        }).withDispatcher(dispatcherKey))

        def receive = Actor.emptyBehavior

      }))

      // Low messages should come out first, and in priority order.  High messages follow - they are equal priority and
      // should come out in the same order in which they were sent.
      val lo = (1 to 100) toList
      val hi = shuffled.filter { _ > 100 }
      (expectMsgType[List[Int]]: List[Int]) should ===(lo ++ hi)
    }
  }
}
