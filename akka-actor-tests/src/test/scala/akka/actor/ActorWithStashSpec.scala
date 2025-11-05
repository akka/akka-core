/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.actor

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import language.postfixOps
import org.scalatest.BeforeAndAfterEach

import akka.pattern.ask
import akka.testkit._
import akka.testkit.DefaultTimeout
import akka.testkit.TestEvent._

object ActorWithStashSpec {

  class StashingActor extends Actor with Stash {
    import context.system
    def greeted: Receive = {
      case "bye" =>
        state.s = "bye"
        state.finished.await()
      case _ => // do nothing
    }

    def receive = {
      case "hello" =>
        state.s = "hello"
        unstashAll()
        context.become(greeted)
      case _ => stash()
    }
  }

  class StashingTwiceActor extends Actor with Stash {
    def receive = {
      case "hello" =>
        try {
          stash()
          stash()
        } catch {
          case _: IllegalStateException =>
            state.expectedException.open()
        }
      case _ => // do nothing
    }
  }

  class ActorWithProtocol extends Actor with Stash {
    import context.system
    def receive = {
      case "open" =>
        unstashAll()
        context.become {
          case "write" => // do writing...
          case "close" =>
            unstashAll()
            context.unbecome()
          case _ => stash()
        }
      case "done" => state.finished.await()
      case _      => stash()
    }
  }

  class WatchedActor extends Actor {
    def receive = Actor.emptyBehavior
  }

  class TerminatedMessageStashingActor(probe: ActorRef) extends Actor with Stash {
    val watched = context.watch(context.actorOf(Props[WatchedActor]()))
    var stashed = false

    context.stop(watched)

    def receive = {
      case Terminated(`watched`) =>
        if (!stashed) {
          stash()
          stashed = true
          unstashAll()
        }
        probe ! "terminated"
    }
  }

  object state {
    @volatile
    var s: String = ""
    val finished = TestBarrier(2)
    var expectedException: TestLatch = null
  }

}

@nowarn
class ActorWithStashSpec extends AkkaSpec with DefaultTimeout with BeforeAndAfterEach {
  import ActorWithStashSpec._

  override def atStartup(): Unit = {
    system.eventStream.publish(Mute(EventFilter[Exception]("Crashing...")))
  }

  override def beforeEach() = state.finished.reset()

  "An Actor with Stash" must {

    "stash messages" in {
      val stasher = system.actorOf(Props(new StashingActor))
      stasher ! "bye"
      stasher ! "hello"
      state.finished.await()
      state.s should ===("bye")
    }

    "support protocols" in {
      val protoActor = system.actorOf(Props[ActorWithProtocol]())
      protoActor ! "open"
      protoActor ! "write"
      protoActor ! "open"
      protoActor ! "close"
      protoActor ! "write"
      protoActor ! "close"
      protoActor ! "done"
      state.finished.await()
    }

    "throw an IllegalStateException if the same messages is stashed twice" in {
      state.expectedException = new TestLatch
      val stasher = system.actorOf(Props[StashingTwiceActor]())
      stasher ! "hello"
      stasher ! "hello"
      Await.ready(state.expectedException, 10 seconds)
    }

    "process stashed messages after restart" in {
      val boss = system.actorOf(
        Props(
          new Supervisor(OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1 second)(List(classOf[Throwable])))))

      val restartLatch = new TestLatch
      val hasMsgLatch = new TestLatch

      val employeeProps = Props(new Actor with Stash {
        def receive = {
          case "crash" =>
            throw new Exception("Crashing...")

          // when restartLatch is not yet open, stash all messages != "crash"
          case _ if !restartLatch.isOpen =>
            stash()

          // when restartLatch is open, must receive "hello"
          case "hello" =>
            hasMsgLatch.open()
        }

        override def preRestart(reason: Throwable, message: Option[Any]) = {
          if (!restartLatch.isOpen)
            restartLatch.open()
          super.preRestart(reason, message)
        }
      })
      val employee = Await.result((boss ? employeeProps).mapTo[ActorRef], timeout.duration)

      employee ! "hello"
      employee ! "crash"

      Await.ready(restartLatch, 10 seconds)
      Await.ready(hasMsgLatch, 10 seconds)
    }

    "re-receive unstashed Terminated messages" in {
      system.actorOf(Props(classOf[TerminatedMessageStashingActor], testActor))
      expectMsg("terminated")
      expectMsg("terminated")
    }
  }
}
