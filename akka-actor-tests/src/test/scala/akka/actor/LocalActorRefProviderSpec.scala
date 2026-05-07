/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import language.postfixOps

import akka.testkit._
import akka.util.Timeout

object LocalActorRefProviderSpec {
  val config = """
    akka {
      log-dead-letters = on
      actor {
        debug.unhandled = on
        default-dispatcher {
          executor = "thread-pool-executor"
          thread-pool-executor {
            fixed-pool-size = 16
          }
        }
      }
    }
  """
}

@nowarn
class LocalActorRefProviderSpec extends AkkaSpec(LocalActorRefProviderSpec.config) {
  "An LocalActorRefProvider" must {

    "find child actor with URL encoded name" in {
      val childName = "akka%3A%2F%2FClusterSystem%40127.0.0.1%3A2552"
      val a = system.actorOf(Props(new Actor {
        val child = context.actorOf(Props.empty, name = childName)
        def receive = {
          case "lookup" =>
            if (childName == child.path.name) {
              val resolved = system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(child.path)
              sender() ! resolved
            } else sender() ! s"$childName is not ${child.path.name}!"
        }
      }))
      a.tell("lookup", testActor)
      val b = expectMsgType[ActorRef]
      b.isTerminated should ===(false)
      b.path.name should ===(childName)
    }

  }

  // #16757: messages sent to /user should be UnhandledMessages instead of DeadLetters
  "The root guardian in a LocalActorRefProvider" must {
    "not handle messages other than those it will act upon" in {

      val message = "Hello, Mr. Root Guardian"
      val rootGuardian = system.actorSelection("/")
      val deadLettersPath = system.deadLetters.path

      filterEvents(EventFilter.warning(s"unhandled message from Actor[$deadLettersPath]: $message", occurrences = 1)) {
        rootGuardian ! message
      }
    }
  }

  "The user guardian in a LocalActorRefProvider" must {
    "not handle messages other than those it will act upon" in {

      val message = "Hello, Mr. User Guardian"
      val userGuardian = system.actorSelection("/user")
      val deadLettersPath = system.deadLetters.path

      filterEvents(EventFilter.warning(s"unhandled message from Actor[$deadLettersPath]: $message", occurrences = 1)) {
        userGuardian ! message
      }
    }
  }

  "The system guardian in a LocalActorRefProvider" must {
    "not handle messages other than those it will act upon" in {

      val message = "Hello, Mr. System Guardian"
      val systemGuardian = system.actorSelection("/system")
      val deadLettersPath = system.deadLetters.path

      filterEvents(EventFilter.warning(s"unhandled message from Actor[$deadLettersPath]: $message", occurrences = 1)) {
        systemGuardian ! message
      }
    }
  }

  "A LocalActorRef's ActorCell" must {
    "not retain its original Props when terminated" in {
      val GetChild = "GetChild"
      val a = watch(system.actorOf(Props(new Actor {
        val child = context.actorOf(Props.empty)
        def receive = { case `GetChild` => sender() ! child }
      })))
      a.tell(GetChild, testActor)
      val child = expectMsgType[ActorRef]
      val childProps1 = child.asInstanceOf[LocalActorRef].underlying.props
      childProps1 should ===(Props.empty)
      system.stop(a)
      expectTerminated(a)
      // the fields are cleared after the Terminated message has been sent,
      // so we need to check for a reasonable time after we receive it
      awaitAssert({
        val childProps2 = child.asInstanceOf[LocalActorRef].underlying.props
        childProps2 should not be theSameInstanceAs(childProps1)
        (childProps2 should be).theSameInstanceAs(ActorCell.terminatedProps)
      }, 1 second)
    }
  }

  "An ActorRefFactory" must {
    implicit val ec = system.dispatcher
    "only create one instance of an actor with a specific address in a concurrent environment" in {
      val impl = system.asInstanceOf[ActorSystemImpl]
      val provider = impl.provider

      provider.isInstanceOf[LocalActorRefProvider] should ===(true)

      for (i <- 0 until 100) {
        val address = "new-actor" + i
        implicit val timeout: Timeout = Timeout(5 seconds)
        val actors =
          for (_ <- 1 to 4)
            yield Future(system.actorOf(Props(new Actor { def receive = { case _ => } }), address))
        val set: Set[Any] = Set() ++ actors.map(a =>
            Await.ready(a, timeout.duration).value match {
              case Some(Success(_: ActorRef))                  => 1
              case Some(Failure(_: InvalidActorNameException)) => 2
              case x                                           => x
            })
        set should ===(Set[Any](1, 2))
      }
    }

    "only create one instance of an actor from within the same message invocation" in {
      val supervisor = system.actorOf(Props(new Actor {
        def receive = {
          case "" =>
            val a, b = context.actorOf(Props.empty, "duplicate")
        }
      }))
      EventFilter[InvalidActorNameException](occurrences = 1).intercept {
        supervisor ! ""
      }
    }

    "throw suitable exceptions for malformed actor names" in {
      intercept[InvalidActorNameException](system.actorOf(Props.empty, null)).getMessage should include("null")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "")).getMessage should include("empty")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "$hallo")).getMessage should include(
        "not start with `$`")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "a%")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "%3")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "%xx")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "%0G")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "%gg")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "%")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "%1t")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "a?")).getMessage should include(
        "Invalid actor path element")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, "üß")).getMessage should include(
        "include only ASCII")

      intercept[InvalidActorNameException](system.actorOf(Props.empty, """he"llo""")).getMessage should include(
        """["] at position: 2""")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, """$hello""")).getMessage should include(
        """[$] at position: 0""")
      intercept[InvalidActorNameException](system.actorOf(Props.empty, """hell>o""")).getMessage should include(
        """[>] at position: 4""")
    }

  }
}
