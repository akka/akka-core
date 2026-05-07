/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import scala.concurrent.duration._
import scala.concurrent.Future
import akka.actor.typed.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.SupervisorStrategy

import scala.concurrent.duration.FiniteDuration
import akka.Done

//#oo-style
//#fun-style

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
//#fun-style
import akka.actor.typed.scaladsl.AbstractBehavior
import org.slf4j.Logger
//#oo-style

object StyleGuideDocExamples {

  object FunctionalStyle {

    //#fun-style

    //#messages
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)
      //#messages

      def apply(): Behavior[Command] =
        counter(0)

      private def counter(n: Int): Behavior[Command] =
        Behaviors.receive { (context, message) =>
          message match {
            case Increment =>
              val newValue = n + 1
              context.log.debug("Incremented counter to [{}]", newValue)
              counter(newValue)
            case GetValue(replyTo) =>
              replyTo ! Value(n)
              Behaviors.same
          }
        }
      //#messages
    }
    //#messages
    //#fun-style

  }

  object OOStyle {

    //#oo-style

    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      def apply(): Behavior[Command] = {
        Behaviors.setup(context => new Counter(context))
      }
    }

    class Counter(context: ActorContext[Counter.Command]) extends AbstractBehavior[Counter.Command](context) {
      import Counter._

      private var n = 0

      override def onMessage(msg: Command): Behavior[Counter.Command] = {
        msg match {
          case Increment =>
            n += 1
            context.log.debug("Incremented counter to [{}]", n)
            this
          case GetValue(replyTo) =>
            replyTo ! Value(n)
            this
        }
      }
    }
    //#oo-style

  }

  object FunctionalStyleSetupParams1 {
    // #fun-style-setup-params1

    // this is an anti-example, better solutions exists
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class IncrementRepeatedly(interval: FiniteDuration) extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      def apply(name: String): Behavior[Command] =
        Behaviors.withTimers { timers =>
          counter(name, timers, 0)
        }

      private def counter(name: String, timers: TimerScheduler[Command], n: Int): Behavior[Command] =
        Behaviors.receive { (context, message) =>
          message match {
            case IncrementRepeatedly(interval) =>
              context.log.debug(
                "[{}] Starting repeated increments with interval [{}], current count is [{}]",
                name,
                interval.toString,
                n.toString)
              timers.startTimerWithFixedDelay(Increment, interval)
              Behaviors.same
            case Increment =>
              val newValue = n + 1
              context.log.debug("[{}] Incremented counter to [{}]", name, newValue)
              counter(name, timers, newValue)
            case GetValue(replyTo) =>
              replyTo ! Value(n)
              Behaviors.same
          }
        }
    }
    // #fun-style-setup-params1
  }

  object FunctionalStyleSetupParams2 {
    // #fun-style-setup-params2

    // this is better than previous example, but even better solution exists
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class IncrementRepeatedly(interval: FiniteDuration) extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      private case class Setup(name: String, context: ActorContext[Command], timers: TimerScheduler[Command])

      def apply(name: String): Behavior[Command] =
        Behaviors.setup { context =>
          Behaviors.withTimers { timers =>
            counter(Setup(name, context, timers), 0)
          }
        }

      private def counter(setup: Setup, n: Int): Behavior[Command] =
        Behaviors.receiveMessage {
          case IncrementRepeatedly(interval) =>
            setup.context.log.debug(
              "[{}] Starting repeated increments with interval [{}], current count is [{}]",
              setup.name,
              interval,
              n)
            setup.timers.startTimerWithFixedDelay(Increment, interval)
            Behaviors.same
          case Increment =>
            val newValue = n + 1
            setup.context.log.debug("[{}] Incremented counter to [{}]", setup.name, newValue)
            counter(setup, newValue)
          case GetValue(replyTo) =>
            replyTo ! Value(n)
            Behaviors.same
        }
    }
    // #fun-style-setup-params2
  }

  object FunctionalStyleSetupParams3 {
    // #fun-style-setup-params3

    // this is better than previous examples
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class IncrementRepeatedly(interval: FiniteDuration) extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      def apply(name: String): Behavior[Command] =
        Behaviors.setup { context =>
          Behaviors.withTimers { timers =>
            new Counter(name, context, timers).counter(0)
          }
        }
    }

    class Counter private (
        name: String,
        context: ActorContext[Counter.Command],
        timers: TimerScheduler[Counter.Command]) {
      import Counter._

      private def counter(n: Int): Behavior[Command] =
        Behaviors.receiveMessage {
          case IncrementRepeatedly(interval) =>
            context.log.debug(
              "[{}] Starting repeated increments with interval [{}], current count is [{}]",
              name,
              interval,
              n)
            timers.startTimerWithFixedDelay(Increment, interval)
            Behaviors.same
          case Increment =>
            val newValue = n + 1
            context.log.debug("[{}] Incremented counter to [{}]", name, newValue)
            counter(newValue)
          case GetValue(replyTo) =>
            replyTo ! Value(n)
            Behaviors.same
        }
    }
    // #fun-style-setup-params3
  }

  object FunctionalStyleSetupParams4 {
    // #fun-style-setup-params4

    // this works, but previous example is better for structuring more complex behaviors
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class IncrementRepeatedly(interval: FiniteDuration) extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      def apply(name: String): Behavior[Command] =
        Behaviors.setup { context =>
          Behaviors.withTimers { timers =>
            def counter(n: Int): Behavior[Command] =
              Behaviors.receiveMessage {
                case IncrementRepeatedly(interval) =>
                  context.log.debug(
                    "[{}] Starting repeated increments with interval [{}], current count is [{}]",
                    name,
                    interval,
                    n)
                  timers.startTimerWithFixedDelay(Increment, interval)
                  Behaviors.same
                case Increment =>
                  val newValue = n + 1
                  context.log.debug("[{}] Incremented counter to [{}]", name, newValue)
                  counter(newValue)
                case GetValue(replyTo) =>
                  replyTo ! Value(n)
                  Behaviors.same
              }

            counter(0)
          }
        }
    }

    // #fun-style-setup-params4
  }

  object FactoryMethod {
    //#behavior-factory-method
    object CountDown {
      sealed trait Command
      case object Down extends Command

      // factory for the initial `Behavior`
      def apply(countDownFrom: Int, notifyWhenZero: ActorRef[Done]): Behavior[Command] =
        new CountDown(notifyWhenZero).counter(countDownFrom)
    }

    private class CountDown(notifyWhenZero: ActorRef[Done]) {
      import CountDown._

      private def counter(remaining: Int): Behavior[Command] = {
        //#exhastivness-check
        Behaviors.receiveMessage {
          case Down =>
            if (remaining == 1) {
              notifyWhenZero.tell(Done)
              Behaviors.stopped
            } else
              counter(remaining - 1)
        }
        //#exhastivness-check
      }

    }
    //#behavior-factory-method

    object Usage {
      val context: ActorContext[_] = ???
      val doneRef: ActorRef[Done] = ???

      //#behavior-factory-method-spawn
      val countDown = context.spawn(CountDown(100, doneRef), "countDown")
      //#behavior-factory-method-spawn

      //#message-prefix-in-tell
      countDown ! CountDown.Down
      //#message-prefix-in-tell
    }
  }

  object Messages {
    //#message-protocol
    object CounterProtocol {
      sealed trait Command

      final case class Increment(delta: Int, replyTo: ActorRef[OperationResult]) extends Command
      final case class Decrement(delta: Int, replyTo: ActorRef[OperationResult]) extends Command

      sealed trait OperationResult
      case object Confirmed extends OperationResult
      final case class Rejected(reason: String) extends OperationResult
    }
    //#message-protocol
  }

  object PublicVsPrivateMessages1 {
    //#public-private-messages-1
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      // Tick is private so can't be sent from the outside
      private case object Tick extends Command

      def apply(name: String, tickInterval: FiniteDuration): Behavior[Command] =
        Behaviors.setup { context =>
          Behaviors.withTimers { timers =>
            timers.startTimerWithFixedDelay(Tick, tickInterval)
            new Counter(name, context).counter(0)
          }
        }
    }

    class Counter private (name: String, context: ActorContext[Counter.Command]) {
      import Counter._

      private def counter(n: Int): Behavior[Command] =
        Behaviors.receiveMessage {
          case Increment =>
            val newValue = n + 1
            context.log.debug("[{}] Incremented counter to [{}]", name, newValue)
            counter(newValue)
          case Tick =>
            val newValue = n + 1
            context.log.debug("[{}] Incremented counter by background tick to [{}]", name, newValue)
            counter(newValue)
          case GetValue(replyTo) =>
            replyTo ! Value(n)
            Behaviors.same
        }
    }
    //#public-private-messages-1
  }

  object PublicVsPrivateMessages2 {
    //#public-private-messages-2
    // above example is preferred, but this is possible and not wrong
    object Counter {
      // The type of all public and private messages the Counter actor handles
      sealed trait Message

      /** Counter's public message protocol type. */
      sealed trait Command extends Message
      case object Increment extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)

      // The type of the Counter actor's internal messages.
      sealed trait PrivateCommand extends Message
      // Tick is a private command so can't be sent to an ActorRef[Command]
      case object Tick extends PrivateCommand

      def apply(name: String, tickInterval: FiniteDuration): Behavior[Command] = {
        Behaviors
          .setup[Counter.Message] { context =>
            Behaviors.withTimers { timers =>
              timers.startTimerWithFixedDelay(Tick, tickInterval)
              new Counter(name, context).counter(0)
            }
          }
          .narrow // note narrow here
      }
    }

    class Counter private (name: String, context: ActorContext[Counter.Message]) {
      import Counter._

      private def counter(n: Int): Behavior[Message] =
        Behaviors.receiveMessage {
          case Increment =>
            val newValue = n + 1
            context.log.debug("[{}] Incremented counter to [{}]", name, newValue)
            counter(newValue)
          case Tick =>
            val newValue = n + 1
            context.log.debug("[{}] Incremented counter by background tick to [{}]", name, newValue)
            counter(newValue)
          case GetValue(replyTo) =>
            replyTo ! Value(n)
            Behaviors.same
        }
    }
    //#public-private-messages-2
  }

  object Ask {
    import Messages.CounterProtocol._

    implicit val system: ActorSystem[Nothing] = ???

    //#ask-1
    import akka.actor.typed.scaladsl.AskPattern._
    import akka.util.Timeout

    implicit val timeout: Timeout = Timeout(3.seconds)
    val counter: ActorRef[Command] = ???

    val result: Future[OperationResult] = counter.ask(replyTo => Increment(delta = 2, replyTo))
    //#ask-1

    //#ask-2
    val result2: Future[OperationResult] = counter.ask(Increment(delta = 2, _))
    //#ask-2

    /*
    //#ask-3
    // doesn't compile
    val result3: Future[OperationResult] = counter ? Increment(delta = 2, _)
    //#ask-3
     */

    //#ask-4
    val result3: Future[OperationResult] = counter ? (Increment(delta = 2, _))
    //#ask-4
  }

  object ExhaustivenessCheck {

    object CountDown {
      //#messages-sealed
      sealed trait Command
      case object Down extends Command
      final case class GetValue(replyTo: ActorRef[Value]) extends Command
      final case class Value(n: Int)
      //#messages-sealed
    }

    class CountDown() {
      import CountDown._

      //#pattern-match-unhandled
      val zero: Behavior[Command] = {
        Behaviors.receiveMessage {
          case GetValue(replyTo) =>
            replyTo ! Value(0)
            Behaviors.same
          case Down =>
            Behaviors.unhandled
        }
      }
      //#pattern-match-unhandled

      object partial {
        //#pattern-match-partial
        val zero: Behavior[Command] = {
          Behaviors.receiveMessagePartial {
            case GetValue(replyTo) =>
              replyTo ! Value(0)
              Behaviors.same
          }
        }
        //#pattern-match-partial
      }

    }
  }

  object BehaviorCompositionWithPartialFunction {

    //#messages-sealed-composition
    sealed trait Command
    case object Down extends Command
    final case class GetValue(replyTo: ActorRef[Value]) extends Command
    final case class Value(n: Int)
    //#messages-sealed-composition

    //#get-handler-partial
    def getHandler(value: Int): PartialFunction[Command, Behavior[Command]] = {
      case GetValue(replyTo) =>
        replyTo ! Value(value)
        Behaviors.same
    }
    //#get-handler-partial

    //#set-handler-non-zero-partial
    def setHandlerNotZero(value: Int): PartialFunction[Command, Behavior[Command]] = {
      case Down =>
        if (value == 1)
          zero
        else
          nonZero(value - 1)
    }
    //#set-handler-non-zero-partial

    //#set-handler-zero-partial
    def setHandlerZero(log: Logger): PartialFunction[Command, Behavior[Command]] = {
      case Down =>
        log.error("Counter is already at zero!")
        Behaviors.same
    }
    //#set-handler-zero-partial

    //#top-level-behaviors-partial
    val zero: Behavior[Command] = Behaviors.setup { context =>
      Behaviors.receiveMessagePartial(getHandler(0).orElse(setHandlerZero(context.log)))
    }

    def nonZero(capacity: Int): Behavior[Command] =
      Behaviors.receiveMessagePartial(getHandler(capacity).orElse(setHandlerNotZero(capacity)))

    // Default Initial Behavior for this actor
    def apply(initialCapacity: Int): Behavior[Command] = nonZero(initialCapacity)
    //#top-level-behaviors-partial
  }

  object NestingSample1 {
    sealed trait Command

    //#nesting
    def apply(): Behavior[Command] =
      Behaviors.setup[Command](context =>
        Behaviors.withStash(100)(stash =>
          Behaviors.withTimers { timers =>
            context.log.debug("Starting up")

            // behavior using context, stash and timers ...
            //#nesting
            timers.isTimerActive("aa")
            stash.isEmpty
            Behaviors.empty
          //#nesting
          }))
    //#nesting
  }

  object NestingSample2 {
    sealed trait Command

    //#nesting-supervise
    def apply(): Behavior[Command] =
      Behaviors.setup { context =>
        // only run on initial actor start, not on crash-restart
        context.log.info("Starting")

        Behaviors
          .supervise(Behaviors.withStash[Command](100) { stash =>
            // every time the actor crashes and restarts a new stash is created (previous stash is lost)
            context.log.debug("Starting up with stash")
            // Behaviors.receiveMessage { ... }
            //#nesting-supervise
            stash.isEmpty
            Behaviors.empty
            //#nesting-supervise
          })
          .onFailure[RuntimeException](SupervisorStrategy.restart)
      }
    //#nesting-supervise
  }
}
