/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted

// unused names in pattern match can be useful in the docs
@nowarn
object PersistentActorCompileOnlyTest {

  import akka.persistence.typed.scaladsl.EventSourcedBehavior._

  object WithAck {
    case object Ack

    sealed trait MyCommand
    case class Cmd(data: String, sender: ActorRef[Ack.type]) extends MyCommand

    sealed trait MyEvent
    case class Evt(data: String) extends MyEvent

    case class ExampleState(events: List[String] = Nil)

    EventSourcedBehavior[MyCommand, MyEvent, ExampleState](
      persistenceId = PersistenceId.ofUniqueId("sample-id-1"),
      emptyState = ExampleState(Nil),
      commandHandler = CommandHandler.command {
        case Cmd(data, sender) =>
          Effect.persist(Evt(data)).thenRun { _ =>
            sender ! Ack
          }
      },
      eventHandler = {
        case (state, Evt(data)) => state.copy(data :: state.events)
      })
  }

  object RecoveryComplete {
    sealed trait Command
    case class DoSideEffect(data: String) extends Command
    case class AcknowledgeSideEffect(correlationId: Int) extends Command

    sealed trait Event
    case class IntentRecorded(correlationId: Int, data: String) extends Event
    case class SideEffectAcknowledged(correlationId: Int) extends Event

    case class EventsInFlight(nextCorrelationId: Int, dataByCorrelationId: Map[Int, String])

    case class Request(correlationId: Int, data: String, sender: ActorRef[Response])
    case class Response(correlationId: Int)
    val sideEffectProcessor: ActorRef[Request] = ???

    def performSideEffect(sender: ActorRef[AcknowledgeSideEffect], correlationId: Int, data: String): Unit = {
      import akka.actor.typed.scaladsl.AskPattern._
      implicit val timeout: akka.util.Timeout = 1.second
      implicit val system: ActorSystem[_] = ???
      implicit val ec: ExecutionContext = ???

      val response: Future[RecoveryComplete.Response] =
        sideEffectProcessor.ask(Request(correlationId, data, _))

      response.map(response => AcknowledgeSideEffect(response.correlationId)).foreach(sender ! _)
    }

    val behavior: Behavior[Command] =
      Behaviors.setup(
        ctx =>
          EventSourcedBehavior[Command, Event, EventsInFlight](
            persistenceId = PersistenceId.ofUniqueId("recovery-complete-id"),
            emptyState = EventsInFlight(0, Map.empty),
            commandHandler = (state, cmd) =>
              cmd match {
                case DoSideEffect(data) =>
                  Effect.persist(IntentRecorded(state.nextCorrelationId, data)).thenRun { _ =>
                    performSideEffect(ctx.self, state.nextCorrelationId, data)
                  }
                case AcknowledgeSideEffect(correlationId) =>
                  Effect.persist(SideEffectAcknowledged(correlationId))
              },
            eventHandler = (state, evt) =>
              evt match {
                case IntentRecorded(correlationId, data) =>
                  EventsInFlight(
                    nextCorrelationId = correlationId + 1,
                    dataByCorrelationId = state.dataByCorrelationId + (correlationId -> data))
                case SideEffectAcknowledged(correlationId) =>
                  state.copy(dataByCorrelationId = state.dataByCorrelationId - correlationId)
              }).receiveSignal {
            case (state, RecoveryCompleted) =>
              state.dataByCorrelationId.foreach {
                case (correlationId, data) => performSideEffect(ctx.self, correlationId, data)
              }
          })

  }

  object Become {
    sealed trait Mood
    case object Happy extends Mood
    case object Sad extends Mood

    sealed trait Command
    case class Greet(name: String) extends Command
    case object MoodSwing extends Command

    sealed trait Event
    case class MoodChanged(to: Mood) extends Event

    val b: Behavior[Command] = EventSourcedBehavior[Command, Event, Mood](
      persistenceId = PersistenceId.ofUniqueId("myPersistenceId"),
      emptyState = Happy,
      commandHandler = { (state, command) =>
        state match {
          case Happy =>
            command match {
              case Greet(whom) =>
                println(s"Super happy to meet you $whom!")
                Effect.none
              case MoodSwing => Effect.persist(MoodChanged(Sad))
            }
          case Sad =>
            command match {
              case Greet(whom) =>
                println(s"hi $whom")
                Effect.none
              case MoodSwing => Effect.persist(MoodChanged(Happy))
            }
        }
      },
      eventHandler = {
        case (_, MoodChanged(to)) => to
      })

    Behaviors.withTimers((timers: TimerScheduler[Command]) => {
      timers.startTimerWithFixedDelay(MoodSwing, 10.seconds)
      b
    })
  }

  object ExplicitSnapshots {
    type Task = String

    sealed trait Command
    case class RegisterTask(task: Task) extends Command
    case class TaskDone(task: Task) extends Command

    sealed trait Event
    case class TaskRegistered(task: Task) extends Event
    case class TaskRemoved(task: Task) extends Event

    case class State(tasksInFlight: List[Task])

    EventSourcedBehavior[Command, Event, State](
      persistenceId = PersistenceId.ofUniqueId("asdf"),
      emptyState = State(Nil),
      commandHandler = CommandHandler.command {
        case RegisterTask(task) => Effect.persist(TaskRegistered(task))
        case TaskDone(task)     => Effect.persist(TaskRemoved(task))
      },
      eventHandler = (state, evt) =>
        evt match {
          case TaskRegistered(task) => State(task :: state.tasksInFlight)
          case TaskRemoved(task) =>
            State(state.tasksInFlight.filter(_ != task))
        }).snapshotWhen { (state, e, seqNr) =>
      state.tasksInFlight.isEmpty
    }
  }

  object SpawnChild {
    type Task = String
    sealed trait Command
    case class RegisterTask(task: Task) extends Command
    case class TaskDone(task: Task) extends Command

    sealed trait Event
    case class TaskRegistered(task: Task) extends Event
    case class TaskRemoved(task: Task) extends Event

    case class State(tasksInFlight: List[Task])

    def worker(task: Task): Behavior[Nothing] = ???

    val behavior: Behavior[Command] = Behaviors.setup(
      ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = PersistenceId.ofUniqueId("asdf"),
          emptyState = State(Nil),
          commandHandler = (_, cmd) =>
            cmd match {
              case RegisterTask(task) =>
                Effect.persist(TaskRegistered(task)).thenRun { _ =>
                  val child = ctx.spawn[Nothing](worker(task), task)
                  // This assumes *any* termination of the child may trigger a `TaskDone`:
                  ctx.watchWith(child, TaskDone(task))
                }
              case TaskDone(task) => Effect.persist(TaskRemoved(task))
            },
          eventHandler = (state, evt) =>
            evt match {
              case TaskRegistered(task) => State(task :: state.tasksInFlight)
              case TaskRemoved(task) =>
                State(state.tasksInFlight.filter(_ != task))
            }))

  }

  object FactoringOutEventHandling {
    sealed trait Mood
    case object Happy extends Mood
    case object Sad extends Mood

    case object Ack

    sealed trait Command
    case class Greet(name: String) extends Command
    case class CheerUp(sender: ActorRef[Ack.type]) extends Command
    case class Remember(memory: String) extends Command

    sealed trait Event
    case class MoodChanged(to: Mood) extends Event
    case class Remembered(memory: String) extends Event

    def changeMoodIfNeeded(currentState: Mood, newMood: Mood): EffectBuilder[Event, Mood] =
      if (currentState == newMood) Effect.none
      else Effect.persist(MoodChanged(newMood))

    //#commonChainedEffects
    // Example factoring out a chained effect to use in several places with `thenRun`
    val commonChainedEffects: Mood => Unit = _ => println("Command processed")
    // Then in a command handler:
    Effect
      .persist(Remembered("Yep")) // persist event
      .thenRun(commonChainedEffects) // add on common chained effect
    //#commonChainedEffects

    val commandHandler: CommandHandler[Command, Event, Mood] = { (state, cmd) =>
      cmd match {
        case Greet(whom) =>
          println(s"Hi there, I'm $state!")
          Effect.none
        case CheerUp(sender) =>
          changeMoodIfNeeded(state, Happy)
            .thenRun { _ =>
              sender ! Ack
            }
            .thenRun(commonChainedEffects)
        case Remember(memory) =>
          // A more elaborate example to show we still have full control over the effects
          // if needed (e.g. when some logic is factored out but you want to add more effects)
          val commonEffects: EffectBuilder[Event, Mood] = changeMoodIfNeeded(state, Happy)
          Effect.persist(commonEffects.events :+ Remembered(memory)).thenRun(commonChainedEffects)
      }
    }

    private val eventHandler: EventHandler[Mood, Event] = {
      case (_, MoodChanged(to))   => to
      case (state, Remembered(_)) => state
    }

    EventSourcedBehavior[Command, Event, Mood](
      persistenceId = PersistenceId.ofUniqueId("myPersistenceId"),
      emptyState = Sad,
      commandHandler,
      eventHandler)

  }

  object Stopping {
    sealed trait Command
    case object Enough extends Command

    sealed trait Event
    case object Done extends Event

    class State

    private val commandHandler: CommandHandler[Command, Event, State] = CommandHandler.command {
      case Enough =>
        Effect.persist(Done).thenRun((_: State) => println("yay")).thenStop()
    }

    private val eventHandler: (State, Event) => State = {
      case (state, Done) => state
    }

    EventSourcedBehavior[Command, Event, State](
      persistenceId = PersistenceId.ofUniqueId("myPersistenceId"),
      emptyState = new State,
      commandHandler,
      eventHandler)
  }

  object AndThenPatternMatch {
    trait State
    class First extends State
    class Second extends State

    EventSourcedBehavior[String, String, State](
      persistenceId = PersistenceId.ofUniqueId("myPersistenceId"),
      emptyState = new First,
      commandHandler = CommandHandler.command { cmd =>
        Effect.persist(cmd).thenRun {
          case _: First  => println("first")
          case _: Second => println("second")
        }
      },
      eventHandler = {
        case (_: First, _) => new Second
        case (state, _)    => state
      })

  }

}
