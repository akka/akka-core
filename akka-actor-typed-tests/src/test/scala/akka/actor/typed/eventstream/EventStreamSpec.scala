/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.eventstream

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.DeadLetter
import akka.actor.testkit.typed.scaladsl.{ ScalaTestWithActorTestKit, TestProbe }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed.scaladsl.Behaviors

class EventStreamSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import EventStream._
  import EventStreamSpec._

  private final val ShortWait = 100.millis

  "system event stream".can {
    val eventObjListener: TestProbe[EventObj.type] = testKit.createTestProbe()
    val eventClassListener: TestProbe[EventClass] = testKit.createTestProbe()

    "register subscribers" in {
      testKit.system.eventStream ! Subscribe(eventObjListener.ref)
      testKit.system.eventStream ! Subscribe(eventClassListener.ref)
    }

    "accept published events" in {
      testKit.system.eventStream ! Publish(EventObj)
    }
    "dispatch events to subscribers of that type" in {
      eventObjListener.expectMessage(EventObj)
      eventClassListener.expectNoMessage(ShortWait)
      testKit.system.eventStream ! Publish(EventClass())
      eventClassListener.expectMessage(EventClass())
      eventObjListener.expectNoMessage(ShortWait)
    }

    "unsubscribe subscribers" in {
      testKit.system.eventStream ! Unsubscribe(eventObjListener.ref)
      testKit.system.eventStream ! Publish(EventObj)
      eventObjListener.expectNoMessage(ShortWait)
    }

    "be subscribed by message adapter for DeadLetter" in {
      val probe = createTestProbe[String]()
      val ref = testKit.spawn(Behaviors.setup[String] { context =>
        val adapter = context.messageAdapter[DeadLetter](d => d.message.toString)
        context.system.eventStream ! Subscribe(adapter)

        Behaviors.receiveMessage { msg =>
          probe.ref ! msg
          Behaviors.same
        }
      })

      ref ! "init"
      probe.expectMessage("init")
      probe.expectNoMessage(100.millis) // might still be a risk that the eventStream ! Subscribe hasn't arrived

      testKit.system.deadLetters[String] ! "msg1"
      testKit.system.deadLetters[String] ! "msg2"
      testKit.system.deadLetters[String] ! "msg3"

      probe.expectMessage("msg1")
      probe.expectMessage("msg2")
      probe.expectMessage("msg3")
    }

    "not cause infinite loop when stopping subscribed by message adapter for DeadLetter" in {
      val latch = new CountDownLatch(1)
      val ref = testKit.spawn(Behaviors.setup[String] { context =>
        val adapter = context.messageAdapter[DeadLetter](d => d.message.toString)
        context.system.eventStream ! Subscribe(adapter)

        Behaviors.receiveMessage { msg =>
          latch.await(10, TimeUnit.SECONDS)
          if (msg == "stop")
            Behaviors.stopped
          else
            Behaviors.same
        }
      })

      ref ! "msg1"
      ref ! "stop"
      ref ! "msg2"
      ref ! "msg3"
      // msg2 and msg3 in mailbox, will be sent to dead letters
      latch.countDown() // stop

      testKit.createTestProbe().expectTerminated(ref)
    }
  }

  "a system event stream subscriber" must {
    val rootEventListener = testKit.createTestProbe[Root]()
    val level1EventListener = testKit.createTestProbe[Level1]()
    val rootEventListenerForLevel1 = testKit.createTestProbe[Root]()
    testKit.system.eventStream ! Subscribe(rootEventListener.ref)
    testKit.system.eventStream ! Subscribe(level1EventListener.ref)
    testKit.system.eventStream ! Subscribe[Level1](rootEventListenerForLevel1.ref)
    "listen for all subclasses of the events" in {
      testKit.system.eventStream ! Publish(Depth1())
      rootEventListener.expectMessage(Depth1())
      level1EventListener.expectNoMessage(ShortWait)
      rootEventListenerForLevel1.expectNoMessage(ShortWait)

      testKit.system.eventStream ! Publish(Depth2())

      rootEventListener.expectMessage(Depth2())
      level1EventListener.expectMessage(Depth2())
      rootEventListenerForLevel1.expectMessage(Depth2())
    }

  }

  "a system event stream subscriber".can {
    val rootEventListener = testKit.createTestProbe[Root]()

    "subscribe several times between multiple types" in {
      testKit.system.eventStream ! Subscribe[Foo](rootEventListener.ref)
      testKit.system.eventStream ! Subscribe[Bar](rootEventListener.ref)
      testKit.system.eventStream ! Subscribe[Baz](rootEventListener.ref)
      testKit.system.eventStream ! Subscribe[Qux](rootEventListener.ref)
    }

    "listen to events for all subscribed types" in {
      testKit.system.eventStream ! Publish(Foo())
      rootEventListener.expectMessage(Foo())

      testKit.system.eventStream ! Publish(Bar())
      rootEventListener.expectMessage(Bar())

      testKit.system.eventStream ! Publish(Baz())
      rootEventListener.expectMessage(Baz())

      testKit.system.eventStream ! Publish(Qux())
      rootEventListener.expectMessage(Qux())
    }

    "unsubscribe all" in {
      testKit.system.eventStream ! Unsubscribe(rootEventListener.ref)

      testKit.system.eventStream ! Publish(Foo())
      rootEventListener.expectNoMessage(ShortWait)

      testKit.system.eventStream ! Publish(Bar())
      rootEventListener.expectNoMessage(ShortWait)

      testKit.system.eventStream ! Publish(Baz())
      rootEventListener.expectNoMessage(ShortWait)

      testKit.system.eventStream ! Publish(Qux())
      rootEventListener.expectNoMessage(ShortWait)
    }
  }

}

object EventStreamSpec {
  case object EventObj
  case class EventClass()

  sealed trait Root
  case class Depth1() extends Root
  sealed trait Level1 extends Root
  case class Depth2() extends Level1

  case class Foo() extends Root
  case class Bar() extends Root
  case class Baz() extends Root
  case class Qux() extends Root
}
