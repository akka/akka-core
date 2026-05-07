/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.ConfigurationException
import akka.actor.setup.ActorSystemSetup
import akka.dispatch.{ Dispatchers, ExecutionContexts }
import akka.testkit.{ AkkaSpec, ImplicitSender, TestActors, TestProbe }

object ActorSystemDispatchersSpec {

  class SnitchingExecutionContext(testActor: ActorRef, underlying: ExecutionContext) extends ExecutionContext {

    def execute(runnable: Runnable): Unit = {
      testActor ! "called"
      underlying.execute(runnable)
    }

    def reportFailure(t: Throwable): Unit = {
      testActor ! "failed"
      underlying.reportFailure(t)
    }
  }

}

class ActorSystemDispatchersSpec extends AkkaSpec(ConfigFactory.parseString("""
    dispatcher-loop-1 = "dispatcher-loop-2"
    dispatcher-loop-2 = "dispatcher-loop-1"
  """)) with ImplicitSender {

  import ActorSystemDispatchersSpec._

  "The ActorSystem" must {

    "work with a passed in ExecutionContext" in {
      val ecProbe = TestProbe()
      val ec = new SnitchingExecutionContext(ecProbe.ref, ExecutionContexts.global())

      val system2 = ActorSystem(name = "ActorSystemDispatchersSpec-passed-in-ec", defaultExecutionContext = Some(ec))

      try {
        val ref = system2.actorOf(Props(new Actor {
          def receive = {
            case "ping" => sender() ! "pong"
          }
        }))

        val probe = TestProbe()

        ref.tell("ping", probe.ref)

        ecProbe.expectMsg(1.second, "called")
        probe.expectMsg(1.second, "pong")
      } finally {
        shutdown(system2)
      }
    }

    "not use passed in ExecutionContext if executor is configured" in {
      val ecProbe = TestProbe()
      val ec = new SnitchingExecutionContext(ecProbe.ref, ExecutionContexts.global())

      val config = ConfigFactory.parseString("akka.actor.default-dispatcher.executor = \"fork-join-executor\"")
      val system2 = ActorSystem(
        name = "ActorSystemDispatchersSpec-ec-configured",
        config = Some(config),
        defaultExecutionContext = Some(ec))

      try {
        val ref = system2.actorOf(TestActors.echoActorProps)
        val probe = TestProbe()

        ref.tell("ping", probe.ref)

        ecProbe.expectNoMessage()
        probe.expectMsg(1.second, "ping")
      } finally {
        shutdown(system2)
      }
    }

    def userGuardianDispatcher(system: ActorSystem): String = {
      val impl = system.asInstanceOf[ActorSystemImpl]
      impl.guardian.asInstanceOf[ActorRefWithCell].underlying.asInstanceOf[ActorCell].dispatcher.id
    }

    "provide a single place to override the internal dispatcher" in {
      val sys = ActorSystem(
        "ActorSystemDispatchersSpec-override-internal-disp",
        ConfigFactory.parseString("""
             akka.actor.internal-dispatcher = akka.actor.default-dispatcher
           """))
      try {
        // that the user guardian runs on the overridden dispatcher instead of internal
        // isn't really a guarantee any internal actor has been made running on the right one
        // but it's better than no test coverage at all
        userGuardianDispatcher(sys) should ===("akka.actor.default-dispatcher")
      } finally {
        shutdown(sys)
      }
    }

    "provide internal execution context instance through BootstrapSetup" in {
      val ecProbe = TestProbe()
      val ec = new SnitchingExecutionContext(ecProbe.ref, ExecutionContexts.global())

      // using the default for internal dispatcher and passing a pre-existing execution context
      val system2 =
        ActorSystem(
          name = "ActorSystemDispatchersSpec-passed-in-ec-for-internal",
          config = Some(ConfigFactory.parseString("""
            akka.actor.internal-dispatcher = akka.actor.default-dispatcher
          """)),
          defaultExecutionContext = Some(ec))

      try {
        val ref = system2.actorOf(Props(new Actor {
          def receive = {
            case "ping" => sender() ! "pong"
          }
        }).withDispatcher(Dispatchers.InternalDispatcherId))

        val probe = TestProbe()

        ref.tell("ping", probe.ref)

        ecProbe.expectMsg(1.second, "called")
        probe.expectMsg(1.second, "pong")
      } finally {
        shutdown(system2)
      }
    }

    "use an internal dispatcher for the guardian by default" in {
      userGuardianDispatcher(system) should ===("akka.actor.internal-dispatcher")
    }

    "use the default dispatcher by a user provided user guardian" in {
      val sys = new ActorSystemImpl(
        "ActorSystemDispatchersSpec-custom-user-guardian",
        ConfigFactory.defaultReference(),
        getClass.getClassLoader,
        None,
        Some(Props.empty),
        ActorSystemSetup.empty)
      sys.start()
      try {
        userGuardianDispatcher(sys) should ===("akka.actor.default-dispatcher")
      } finally shutdown(sys)
    }

    "provide a good error on an dispatcher alias loop in the config" in {
      intercept[ConfigurationException] {
        system.dispatchers.lookup("dispatcher-loop-1")
      }
    }

  }

}
