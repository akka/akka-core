/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import akka.actor.dungeon.SerializationCheckFailedException
import akka.testkit.AkkaSpec
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender

object FunctionRefSpec {

  case class GetForwarder(replyTo: ActorRef)
  case class DropForwarder(ref: FunctionRef)
  case class Forwarded(msg: Any, sender: ActorRef)

  class Super extends Actor {
    def receive = {
      case GetForwarder(replyTo) =>
        val cell = context.asInstanceOf[ActorCell]
        val ref = cell.addFunctionRef((sender, msg) => replyTo ! Forwarded(msg, sender))
        replyTo ! ref
      case DropForwarder(ref) =>
        val cell = context.asInstanceOf[ActorCell]
        cell.removeFunctionRef(ref)
    }
  }

  class SupSuper extends Actor {
    val s = context.actorOf(Props[Super](), "super")
    def receive = {
      case msg => s ! msg
    }
  }

}

class FunctionRefSpec extends AkkaSpec("""
  # test is using Java serialization and relies on serialize-messages=on
  akka.actor.allow-java-serialization = on
  akka.actor.warn-about-java-serializer-usage = off
  akka.actor.serialize-messages = on
  akka.actor.no-serialization-verification-needed-class-prefix = []
  """) with ImplicitSender {
  import FunctionRefSpec._

  def commonTests(s: ActorRef) = {
    s ! GetForwarder(testActor)
    val forwarder = expectMsgType[FunctionRef]

    "forward messages" in {
      forwarder ! "hello"
      expectMsg(Forwarded("hello", testActor))
    }

    "be watchable" in {
      s ! GetForwarder(testActor)
      val f = expectMsgType[FunctionRef]
      watch(f)
      s ! DropForwarder(f)
      expectTerminated(f)
    }

    "be able to watch" in {
      s ! GetForwarder(testActor)
      val f = expectMsgType[FunctionRef]
      forwarder.watch(f)
      forwarder.isWatching(f) should ===(true)
      s ! DropForwarder(f)
      expectMsg(Forwarded(Terminated(f)(true, false), f))

      // Upon receiving the Terminated message, unwatch() must be called, which is different from an ordinary actor.
      forwarder.isWatching(f) should ===(true)
      forwarder.unwatch(f)
      forwarder.isWatching(f) should ===(false)
    }

    "terminate when their parent terminates" in {
      watch(forwarder)
      s ! PoisonPill
      expectTerminated(forwarder)
    }
  }

  "A FunctionRef" when {

    "created by a toplevel actor" must {
      val s = system.actorOf(Props[Super](), "super")
      commonTests(s)
    }

    "created by a non-toplevel actor" must {
      val s = system.actorOf(Props[SupSuper](), "supsuper")
      commonTests(s)
    }

    "not registered" must {
      "not be found" in {
        val provider = system.asInstanceOf[ExtendedActorSystem].provider
        val ref = new FunctionRef(testActor.path / "blabla", provider, system, (_, _) => ())
        EventFilter[SerializationCheckFailedException](
          start = "Failed to serialize and deserialize message of type akka.actor.FunctionRefSpec",
          occurrences = 1).intercept {
          // needs to be something that fails when the deserialized form is not a FunctionRef
          // this relies upon serialize-messages during tests
          testActor ! DropForwarder(ref)
          expectNoMessage()
        }
      }
    }

  }
}
