/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.concurrent._
import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._

object WatchSpec {

  case object Stop

  val terminatorBehavior =
    Behaviors.receive[Stop.type] {
      case (_, Stop) => Behaviors.stopped
    }

  val mutableTerminatorBehavior = Behaviors.setup[Stop.type] { context =>
    new AbstractBehavior[Stop.type](context) {
      override def onMessage(message: Stop.type) = message match {
        case Stop => Behaviors.stopped
      }
    }
  }

  sealed trait Message
  sealed trait CustomTerminationMessage extends Message
  case object CustomTerminationMessage extends CustomTerminationMessage
  case object CustomTerminationMessage2 extends CustomTerminationMessage
  case class StartWatching(watchee: ActorRef[Stop.type]) extends Message
  case class StartWatchingWith(watchee: ActorRef[Stop.type], message: CustomTerminationMessage) extends Message
}

class WatchSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  implicit def classicSystem: actor.ActorSystem = system.toClassic

  import WatchSpec._

  class WatchSetup {
    val terminator = spawn(terminatorBehavior)
    val receivedTerminationSignal: Promise[Terminated] = Promise()
    val watchProbe = TestProbe[Done]()

    val watcher = spawn(
      Behaviors
        .supervise(Behaviors
          .receive[StartWatching] {
            case (context, StartWatching(watchee)) =>
              context.watch(watchee)
              watchProbe.ref ! Done
              Behaviors.same
          }
          .receiveSignal {
            case (_, t: Terminated) =>
              receivedTerminationSignal.success(t)
              Behaviors.stopped
          })
        .onFailure[Throwable](SupervisorStrategy.stop))
  }

  "Actor monitoring" must {

    "get notified of graceful actor termination" in new WatchSetup {
      watcher ! StartWatching(terminator)
      watchProbe.expectMessage(Done)
      terminator ! Stop

      val termination = receivedTerminationSignal.future.futureValue
      termination.ref shouldEqual terminator
    }

    case class HasTerminated(t: Terminated) // we need to wrap it as it is handled specially
    case class ChildHasFailed(t: ChildFailed) // we need to wrap it as it is handled specially

    "notify a parent of child termination because of failure" in {
      val probe = TestProbe[Any]()
      val ex = new TestException("boom")
      val parent = spawn(
        Behaviors.setup[Any] { context =>
          val child = context.spawn(Behaviors.receive[Any]((_, _) => throw ex), "child")
          context.watch(child)

          Behaviors
            .receive[Any] { (_, message) =>
              child ! message
              Behaviors.same
            }
            .receiveSignal {
              case (_, t: ChildFailed) =>
                probe.ref ! ChildHasFailed(t)
                Behaviors.same
              case (_, t: Terminated) =>
                probe.ref ! HasTerminated(t)
                Behaviors.same
            }
        },
        "supervised-child-parent")

      LoggingTestKit.error[TestException].expect {
        parent ! "boom"
      }
      probe.expectMessageType[ChildHasFailed].t.cause shouldEqual ex
    }

    "notify a parent of child termination because of failure with a supervisor" in {
      val probe = TestProbe[Any]()
      val ex = TestException("boom")
      val behavior = Behaviors.setup[Any] { context =>
        val child = context.spawn(
          Behaviors
            .supervise(Behaviors.receive[Any]((_, _) => {
              throw ex
            }))
            .onFailure[Throwable](SupervisorStrategy.stop),
          "child")
        context.watch(child)

        Behaviors
          .receive[Any] { (_, message) =>
            child ! message
            Behaviors.same
          }
          .receiveSignal {
            case (_, t: ChildFailed) =>
              probe.ref ! ChildHasFailed(t)
              Behaviors.same
            case (_, t: Terminated) =>
              probe.ref ! HasTerminated(t)
              Behaviors.same
          }
      }
      val parent = spawn(behavior, "parent")

      LoggingTestKit.error[TestException].expect {
        parent ! "boom"
      }
      probe.expectMessageType[ChildHasFailed].t.cause shouldEqual ex
    }

    "fail the actor itself with DeathPact if it does not accept Terminated" in {
      case class Failed(t: Terminated) // we need to wrap it as it is handled specially
      val probe = TestProbe[Any]()
      val ex = new TestException("boom")
      val grossoBosso =
        spawn(
          Behaviors.setup[Any] { context =>
            val middleManagement = context.spawn(Behaviors.setup[Any] { context =>
              val sixPackJoe = context.spawn(Behaviors.receive[Any]((_, _) => throw ex), "joe")
              context.watch(sixPackJoe)

              Behaviors.receive[Any] { (_, message) =>
                sixPackJoe ! message
                Behaviors.same
              } // no handling of terminated, even though we watched!!!
            }, "middle-management")

            context.watch(middleManagement)

            Behaviors
              .receive[Any] { (_, message) =>
                middleManagement ! message
                Behaviors.same
              }
              .receiveSignal {
                case (_, t: Terminated) =>
                  probe.ref ! Failed(t)
                  Behaviors.stopped
              }

          },
          "grosso-bosso")

      LoggingTestKit.error[TestException].expect {
        LoggingTestKit.error[DeathPactException].expect {
          grossoBosso ! "boom"
        }
      }
      probe.expectMessageType[Failed]
    }

    "allow idempotent invocations of watch" in new WatchSetup {
      watcher ! StartWatching(terminator)
      watchProbe.expectMessage(Done)
      // shouldn't fail when watched twice
      watcher ! StartWatching(terminator)
      watchProbe.expectMessage(Done)
      terminator ! Stop
      receivedTerminationSignal.future.futureValue.ref shouldEqual terminator
    }

    class WatchWithSetup {
      val terminator = spawn(terminatorBehavior)
      val receivedTerminationSignal: Promise[Message] = Promise()
      val watchProbe = TestProbe[Done]()

      val watcher = spawn(
        Behaviors
          .supervise(Behaviors.receive[Message] {
            case (context, StartWatchingWith(watchee, message)) =>
              context.watchWith(watchee, message)
              watchProbe.ref ! Done
              Behaviors.same
            case (_, message) =>
              receivedTerminationSignal.success(message)
              Behaviors.stopped
          })
          .onFailure[Throwable](SupervisorStrategy.stop))
    }
    "get notified of actor termination with a custom message" in new WatchWithSetup {
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)
      watchProbe.expectMessage(Done)
      terminator ! Stop

      receivedTerminationSignal.future.futureValue shouldEqual CustomTerminationMessage
    }
    "allow idempotent invocations of watchWith with matching msgs" in new WatchWithSetup {
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)
      watchProbe.expectMessage(Done)
      // shouldn't fail when watchWith'd twice
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)
      watchProbe.expectMessage(Done)
      terminator ! Stop

      receivedTerminationSignal.future.futureValue shouldEqual CustomTerminationMessage
    }

    "allow watch message definition after watch using unwatch" in {
      val terminator = spawn(terminatorBehavior)
      val receivedTerminationSignal: Promise[Message] = Promise()
      val watchProbe = TestProbe[Done]()

      val watcher = spawn(
        Behaviors
          .supervise(Behaviors.receive[Message] {
            case (context, StartWatching(watchee)) =>
              context.watch(watchee)
              Behaviors.same
            case (context, StartWatchingWith(watchee, message)) =>
              context.unwatch(watchee)
              context.watchWith(watchee, message)
              watchProbe.ref ! Done
              Behaviors.same
            case (_, message) =>
              receivedTerminationSignal.success(message)
              Behaviors.stopped
          })
          .onFailure[Throwable](SupervisorStrategy.stop))

      watcher ! StartWatching(terminator)
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)
      watchProbe.expectMessage(Done)
      terminator ! Stop

      receivedTerminationSignal.future.futureValue shouldEqual CustomTerminationMessage
    }

    "allow watch message redefinition using unwatch" in {
      val terminator = spawn(terminatorBehavior)
      val receivedTerminationSignal: Promise[Message] = Promise()
      val watchProbe = TestProbe[Done]()

      val watcher = spawn(
        Behaviors
          .supervise(Behaviors.receive[Message] {
            case (context, StartWatchingWith(watchee, message)) =>
              context.unwatch(watchee)
              context.watchWith(watchee, message)
              watchProbe.ref ! Done
              Behaviors.same
            case (_, message) =>
              receivedTerminationSignal.success(message)
              Behaviors.stopped
          })
          .onFailure[Throwable](SupervisorStrategy.stop))

      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage2)
      watchProbe.expectMessage(Done)
      watchProbe.expectMessage(Done)
      terminator ! Stop

      receivedTerminationSignal.future.futureValue shouldEqual CustomTerminationMessage2
    }

    class ErrorTestSetup {
      val terminator = spawn(terminatorBehavior)
      private val stopProbe = TestProbe[Done]()

      val watcher = spawn(
        Behaviors
          .supervise(Behaviors
            .receive[Message] {
              case (context, StartWatchingWith(watchee, message)) =>
                context.watchWith(watchee, message)
                Behaviors.same
              case (context, StartWatching(watchee)) =>
                context.watch(watchee)
                Behaviors.same
              case (_, _) =>
                Behaviors.stopped
            }
            .receiveSignal {
              case (_, PostStop) =>
                Behaviors.stopped
            })
          .onFailure[Throwable](SupervisorStrategy.stop))

      def expectStopped(): Unit = stopProbe.expectTerminated(watcher, 1.second)
    }

    "fail when watch is used after watchWith on same subject" in new ErrorTestSetup {
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)

      LoggingTestKit
        .error[IllegalStateException]
        .withMessageContains("termination message was not overwritten")
        .expect {
          watcher ! StartWatching(terminator)
        }
      // supervisor should have stopped the actor
      expectStopped()
    }

    "fail when watchWitch is used after watchWith with different termination message" in new ErrorTestSetup {
      watcher ! StartWatchingWith(terminator, CustomTerminationMessage)

      LoggingTestKit
        .error[IllegalStateException]
        .withMessageContains("termination message was not overwritten")
        .expect {
          watcher ! StartWatchingWith(terminator, CustomTerminationMessage2)
        }
      // supervisor should have stopped the actor
      expectStopped()
    }
    "fail when watchWith is used after watch on same subject" in new ErrorTestSetup {
      watcher ! StartWatching(terminator)

      LoggingTestKit
        .error[IllegalStateException]
        .withMessageContains("termination message was not overwritten")
        .expect {
          watcher ! StartWatchingWith(terminator, CustomTerminationMessage)
        }
      // supervisor should have stopped the actor
      expectStopped()
    }
  }
}
