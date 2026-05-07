/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.DeleteEventsFailed
import akka.persistence.typed.DeleteSnapshotsFailed
import akka.persistence.typed.EventAdapter
import akka.persistence.typed.EventSeq
import akka.persistence.typed.scaladsl.Recovery
//#structure
//#behavior
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.PersistenceId

//#behavior
//#structure
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.SnapshotFailed
import scala.annotation.nowarn

// unused variables in pattern match are useful in the docs
@nowarn
object BasicPersistentBehaviorCompileOnly {

  import akka.persistence.typed.scaladsl.RetentionCriteria

  object FirstExample {
    //#command
    sealed trait Command
    final case class Add(data: String) extends Command
    case object Clear extends Command

    sealed trait Event
    final case class Added(data: String) extends Event
    case object Cleared extends Event
    //#command

    //#state
    final case class State(history: List[String] = Nil)
    //#state

    //#command-handler
    import akka.persistence.typed.scaladsl.Effect

    val commandHandler: (State, Command) => Effect[Event, State] = { (state, command) =>
      command match {
        case Add(data) => Effect.persist(Added(data))
        case Clear     => Effect.persist(Cleared)
      }
    }
    //#command-handler

    //#effects
    def onCommand(subscriber: ActorRef[State], state: State, command: Command): Effect[Event, State] = {
      command match {
        case Add(data) =>
          Effect.persist(Added(data)).thenRun(newState => subscriber ! newState)
        case Clear =>
          Effect.persist(Cleared).thenRun((newState: State) => subscriber ! newState).thenStop()
      }
    }
    //#effects

    //#event-handler
    val eventHandler: (State, Event) => State = { (state, event) =>
      event match {
        case Added(data) => state.copy((data :: state.history).take(5))
        case Cleared     => State(Nil)
      }
    }
    //#event-handler

    //#behavior
    def apply(id: String): Behavior[Command] =
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId.ofUniqueId(id),
        emptyState = State(Nil),
        commandHandler = commandHandler,
        eventHandler = eventHandler)
    //#behavior

  }

  //#structure
  object MyPersistentBehavior {
    sealed trait Command
    sealed trait Event
    final case class State()

    def apply(): Behavior[Command] =
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId.ofUniqueId("abc"),
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
  }
  //#structure

  import MyPersistentBehavior._

  object RecoveryBehavior {
    def apply(persistenceId: PersistenceId): Behavior[Command] =
      //#recovery
      EventSourcedBehavior[Command, Event, State](
        persistenceId = persistenceId,
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
        .receiveSignal {
          case (state, RecoveryCompleted) =>
            throw new NotImplementedError("TODO: add some end-of-recovery side-effect here")
        }
    //#recovery
  }

  object RecoveryDisabledBehavior {
    def apply(): Behavior[Command] =
      //#recovery-disabled
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId.ofUniqueId("abc"),
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
        .withRecovery(Recovery.disabled)
    //#recovery-disabled
  }

  object ReplayLastBehavior {
    def apply(): Behavior[Command] =
      //#replay-last
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId.ofUniqueId("abc"),
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
        .withRecovery(Recovery.replayOnlyLast)
    //#replay-last
  }

  object TaggingBehavior {
    def apply(): Behavior[Command] =
      //#tagging
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId.ofUniqueId("abc"),
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
        .withTagger(_ => Set("tag1", "tag2"))
    //#tagging
  }

  object TaggingBehavior2 {
    sealed trait OrderCompleted extends Event

    //#tagging-query
    val NumberOfEntityGroups = 10

    def tagEvent(entityId: String, event: Event): Set[String] = {
      val entityGroup = s"group-${math.abs(entityId.hashCode % NumberOfEntityGroups)}"
      event match {
        case _: OrderCompleted => Set(entityGroup, "order-completed")
        case _                 => Set(entityGroup)
      }
    }

    def apply(entityId: String): Behavior[Command] = {
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId("ShoppingCart", entityId),
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
        .withTagger(event => tagEvent(entityId, event))
    }
    //#tagging-query
  }

  object WrapBehavior {
    def apply(): Behavior[Command] =
      //#wrapPersistentBehavior
      Behaviors.setup[Command] { context =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = PersistenceId.ofUniqueId("abc"),
          emptyState = State(),
          commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
          eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
          .snapshotWhen((state, _, _) => {
            context.log.info("Snapshot actor {} => state: {}", context.self.path.name, state)
            true
          })
      }
    //#wrapPersistentBehavior
  }

  object Supervision {
    def apply(): Behavior[Command] =
      //#supervision
      EventSourcedBehavior[Command, Event, State](
        persistenceId = PersistenceId.ofUniqueId("abc"),
        emptyState = State(),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
        eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
        .onPersistFailure(
          SupervisorStrategy.restartWithBackoff(minBackoff = 10.seconds, maxBackoff = 60.seconds, randomFactor = 0.1))
    //#supervision
  }

  object BehaviorWithContext {
    // #actor-context
    import akka.persistence.typed.scaladsl.Effect
    import akka.persistence.typed.scaladsl.EventSourcedBehavior.CommandHandler

    def apply(): Behavior[String] =
      Behaviors.setup { context =>
        EventSourcedBehavior[String, String, State](
          persistenceId = PersistenceId.ofUniqueId("myPersistenceId"),
          emptyState = State(),
          commandHandler = CommandHandler.command { cmd =>
            context.log.info("Got command {}", cmd)
            Effect.none
          },
          eventHandler = {
            case (state, _) => state
          })
      }
    // #actor-context
  }

  final case class BookingCompleted(orderNr: String) extends Event

  //#snapshottingEveryN

  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
    .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 1000, keepNSnapshots = 2))
  //#snapshottingEveryN

  //#snapshottingPredicate
  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
    .snapshotWhen {
      case (state, BookingCompleted(_), sequenceNumber) => true
      case (state, event, sequenceNumber)               => false
    }
  //#snapshottingPredicate

  def predicate[State, Event]: (State, Event, Long) => Boolean = { (_, _, _) =>
    true
  }
  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
  //#snapshottingPredicateDeleteEvents
    .snapshotWhen(predicate, deleteEventsOnSnapshot = true)
  //#snapshottingPredicateDeleteEvents

  //#snapshotSelection
  import akka.persistence.typed.SnapshotSelectionCriteria

  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
    .withRecovery(Recovery.withSnapshotSelectionCriteria(SnapshotSelectionCriteria.none))
  //#snapshotSelection

  //#retentionCriteria

  import akka.persistence.typed.scaladsl.Effect

  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => state) // do something based on a particular state
    .snapshotWhen {
      case (state, BookingCompleted(_), sequenceNumber) => true
      case (state, event, sequenceNumber)               => false
    }
    .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
  //#retentionCriteria

  //#snapshotAndEventDeletes

  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
    .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2).withDeleteEventsOnSnapshot)
    .receiveSignal { // optionally respond to signals
      case (state, _: SnapshotFailed)        => // react to failure
      case (state, _: DeleteSnapshotsFailed) => // react to failure
      case (state, _: DeleteEventsFailed)    => // react to failure
    }
  //#snapshotAndEventDeletes

  //#retentionCriteriaWithSignals

  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
    .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
    .receiveSignal { // optionally respond to signals
      case (state, _: SnapshotFailed)        => // react to failure
      case (state, _: DeleteSnapshotsFailed) => // react to failure
    }
  //#retentionCriteriaWithSignals

  //#event-wrapper
  case class Wrapper[T](event: T)
  class WrapperEventAdapter[T] extends EventAdapter[T, Wrapper[T]] {
    override def toJournal(e: T): Wrapper[T] = Wrapper(e)
    override def fromJournal(p: Wrapper[T], manifest: String): EventSeq[T] = EventSeq.single(p.event)
    override def manifest(event: T): String = ""
  }
  //#event-wrapper

  //#install-event-adapter
  EventSourcedBehavior[Command, Event, State](
    persistenceId = PersistenceId.ofUniqueId("abc"),
    emptyState = State(),
    commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"),
    eventHandler = (state, evt) => throw new NotImplementedError("TODO: process the event return the next state"))
    .eventAdapter(new WrapperEventAdapter[Event])
    //#install-event-adapter
    //#custom-stash-buffer
    .withStashCapacity(100)
  //#custom-stash-buffer

}
