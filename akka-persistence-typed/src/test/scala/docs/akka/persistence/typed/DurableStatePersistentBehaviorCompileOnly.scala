/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import akka.actor.typed.ActorRef
import akka.Done
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.state.scaladsl.Effect

//#structure
//#behavior
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.PersistenceId

//#behavior
//#structure

import scala.annotation.nowarn
import akka.serialization.jackson.CborSerializable

// unused variables in pattern match are useful in the docs
@nowarn
object DurableStatePersistentBehaviorCompileOnly {
  object FirstExample {
    //#command
    sealed trait Command[ReplyMessage] extends CborSerializable
    final case object Increment extends Command[Nothing]
    final case class IncrementBy(value: Int) extends Command[Nothing]
    final case class GetValue(replyTo: ActorRef[State]) extends Command[State]
    final case object Delete extends Command[Nothing]
    //#command

    //#state
    final case class State(value: Int) extends CborSerializable
    //#state

    //#command-handler
    import akka.persistence.typed.state.scaladsl.Effect

    val commandHandler: (State, Command[_]) => Effect[State] = (state, command) =>
      command match {
        case Increment         => Effect.persist(state.copy(value = state.value + 1))
        case IncrementBy(by)   => Effect.persist(state.copy(value = state.value + by))
        case GetValue(replyTo) => Effect.reply(replyTo)(state)
        case Delete            => Effect.delete[State]()
      }
    //#command-handler

    //#behavior
    def counter(id: String): DurableStateBehavior[Command[_], State] = {
      DurableStateBehavior.apply[Command[_], State](
        persistenceId = PersistenceId.ofUniqueId(id),
        emptyState = State(0),
        commandHandler = commandHandler)
    }
    //#behavior
  }

  //#structure
  object MyPersistentCounter {
    sealed trait Command[ReplyMessage] extends CborSerializable

    final case class State(value: Int) extends CborSerializable

    def counter(persistenceId: PersistenceId): DurableStateBehavior[Command[_], State] = {
      DurableStateBehavior.apply[Command[_], State](
        persistenceId,
        emptyState = State(0),
        commandHandler =
          (state, command) => throw new NotImplementedError("TODO: process the command & return an Effect"))
    }
  }
  //#structure

  import MyPersistentCounter._

  object MyPersistentCounterWithReplies {

    //#effects
    sealed trait Command[ReplyMessage] extends CborSerializable
    final case class IncrementWithConfirmation(replyTo: ActorRef[Done]) extends Command[Done]
    final case class GetValue(replyTo: ActorRef[State]) extends Command[State]

    final case class State(value: Int) extends CborSerializable

    def counter(persistenceId: PersistenceId): DurableStateBehavior[Command[_], State] = {
      DurableStateBehavior.withEnforcedReplies[Command[_], State](
        persistenceId,
        emptyState = State(0),
        commandHandler = (state, command) =>
          command match {

            case IncrementWithConfirmation(replyTo) =>
              Effect.persist(state.copy(value = state.value + 1)).thenReply(replyTo)(_ => Done)

            case GetValue(replyTo) =>
              Effect.reply(replyTo)(state)
          })
    }
    //#effects
  }

  object BehaviorWithContext {
    // #actor-context
    import akka.persistence.typed.state.scaladsl.Effect
    import akka.persistence.typed.state.scaladsl.DurableStateBehavior.CommandHandler

    def apply(): Behavior[String] =
      Behaviors.setup { context =>
        DurableStateBehavior[String, State](
          persistenceId = PersistenceId.ofUniqueId("myPersistenceId"),
          emptyState = State(0),
          commandHandler = CommandHandler.command { cmd =>
            context.log.info("Got command {}", cmd)
            Effect.none
          })
      }
    // #actor-context
  }

  object TaggingBehavior {
    def apply(): Behavior[Command[_]] =
      //#tagging
      DurableStateBehavior[Command[_], State](
        persistenceId = PersistenceId.ofUniqueId("abc"),
        emptyState = State(0),
        commandHandler = (state, cmd) => throw new NotImplementedError("TODO: process the command & return an Effect"))
        .withTag("tag1")
    //#tagging
  }

  object WrapBehavior {
    import akka.persistence.typed.state.scaladsl.Effect
    import akka.persistence.typed.state.scaladsl.DurableStateBehavior.CommandHandler

    def apply(): Behavior[Command[_]] =
      //#wrapPersistentBehavior
      Behaviors.setup[Command[_]] { context =>
        DurableStateBehavior[Command[_], State](
          persistenceId = PersistenceId.ofUniqueId("abc"),
          emptyState = State(0),
          commandHandler = CommandHandler.command { cmd =>
            context.log.info("Got command {}", cmd)
            Effect.none
          })
      }
    //#wrapPersistentBehavior
  }
}
