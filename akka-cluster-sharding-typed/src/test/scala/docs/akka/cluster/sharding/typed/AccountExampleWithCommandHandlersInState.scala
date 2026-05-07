/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.serialization.jackson.CborSerializable

/**
 * Bank account example illustrating:
 * - different state classes representing the lifecycle of the account
 * - event handlers in the state classes
 * - command handlers in the state classes
 * - replies of various types, using withEnforcedReplies
 */
object AccountExampleWithCommandHandlersInState {

  //#account-entity
  object AccountEntity {
    // Command
    sealed trait Command extends CborSerializable
    final case class CreateAccount(replyTo: ActorRef[StatusReply[Done]]) extends Command
    final case class Deposit(amount: BigDecimal, replyTo: ActorRef[StatusReply[Done]]) extends Command
    final case class Withdraw(amount: BigDecimal, replyTo: ActorRef[StatusReply[Done]]) extends Command
    final case class GetBalance(replyTo: ActorRef[CurrentBalance]) extends Command
    final case class CloseAccount(replyTo: ActorRef[StatusReply[Done]]) extends Command

    // Reply
    final case class CurrentBalance(balance: BigDecimal)

    // Event
    sealed trait Event extends CborSerializable
    case object AccountCreated extends Event
    case class Deposited(amount: BigDecimal) extends Event
    case class Withdrawn(amount: BigDecimal) extends Event
    case object AccountClosed extends Event

    val Zero = BigDecimal(0)

    // type alias to reduce boilerplate
    type ReplyEffect = akka.persistence.typed.scaladsl.ReplyEffect[Event, Account]

    // State
    sealed trait Account extends CborSerializable {
      def applyCommand(cmd: Command): ReplyEffect
      def applyEvent(event: Event): Account
    }
    case object EmptyAccount extends Account {
      override def applyCommand(cmd: Command): ReplyEffect =
        cmd match {
          case CreateAccount(replyTo) =>
            Effect.persist(AccountCreated).thenReply(replyTo)(_ => StatusReply.Ack)
          case _ =>
            // CreateAccount before handling any other commands
            Effect.unhandled.thenNoReply()
        }

      override def applyEvent(event: Event): Account =
        event match {
          case AccountCreated => OpenedAccount(Zero)
          case _              => throw new IllegalStateException(s"unexpected event [$event] in state [EmptyAccount]")
        }
    }
    case class OpenedAccount(balance: BigDecimal) extends Account {
      require(balance >= Zero, "Account balance can't be negative")

      override def applyCommand(cmd: Command): ReplyEffect =
        cmd match {
          case Deposit(amount, replyTo) =>
            Effect.persist(Deposited(amount)).thenReply(replyTo)(_ => StatusReply.Ack)

          case Withdraw(amount, replyTo) =>
            if (canWithdraw(amount))
              Effect.persist(Withdrawn(amount)).thenReply(replyTo)(_ => StatusReply.Ack)
            else
              Effect.reply(replyTo)(StatusReply.Error(s"Insufficient balance $balance to be able to withdraw $amount"))

          case GetBalance(replyTo) =>
            Effect.reply(replyTo)(CurrentBalance(balance))

          case CloseAccount(replyTo) =>
            if (balance == Zero)
              Effect.persist(AccountClosed).thenReply(replyTo)(_ => StatusReply.Ack)
            else
              Effect.reply(replyTo)(StatusReply.Error("Can't close account with non-zero balance"))

          case CreateAccount(replyTo) =>
            Effect.reply(replyTo)(StatusReply.Error("Account is already created"))

        }

      override def applyEvent(event: Event): Account =
        event match {
          case Deposited(amount) => copy(balance = balance + amount)
          case Withdrawn(amount) => copy(balance = balance - amount)
          case AccountClosed     => ClosedAccount
          case AccountCreated    => throw new IllegalStateException(s"unexpected event [$event] in state [OpenedAccount]")
        }

      def canWithdraw(amount: BigDecimal): Boolean = {
        balance - amount >= Zero
      }

    }
    case object ClosedAccount extends Account {
      override def applyCommand(cmd: Command): ReplyEffect =
        cmd match {
          case c: Deposit =>
            replyClosed(c.replyTo)
          case c: Withdraw =>
            replyClosed(c.replyTo)
          case GetBalance(replyTo) =>
            Effect.reply(replyTo)(CurrentBalance(Zero))
          case CloseAccount(replyTo) =>
            replyClosed(replyTo)
          case CreateAccount(replyTo) =>
            replyClosed(replyTo)
        }

      private def replyClosed(replyTo: ActorRef[StatusReply[Done]]): ReplyEffect =
        Effect.reply(replyTo)(StatusReply.Error(s"Account is closed"))

      override def applyEvent(event: Event): Account =
        throw new IllegalStateException(s"unexpected event [$event] in state [ClosedAccount]")
    }

    // when used with sharding, this TypeKey can be used in `sharding.init` and `sharding.entityRefFor`:
    val TypeKey: EntityTypeKey[Command] =
      EntityTypeKey[Command]("Account")

    def apply(persistenceId: PersistenceId): Behavior[Command] = {
      EventSourcedBehavior.withEnforcedReplies[Command, Event, Account](
        persistenceId,
        EmptyAccount,
        (state, cmd) => state.applyCommand(cmd),
        (state, event) => state.applyEvent(event))
    }

  }
  //#account-entity

}
