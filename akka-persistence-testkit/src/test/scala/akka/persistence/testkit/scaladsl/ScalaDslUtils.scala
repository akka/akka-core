/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import akka.actor.typed.ActorRef
import akka.persistence.testkit.{
  Cmd,
  CommonUtils,
  EmptyState,
  Evt,
  NonEmptyState,
  Passivate,
  Recovered,
  Stopped,
  TestCommand
}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }

trait ScalaDslUtils extends CommonUtils {

  def eventSourcedBehavior(pid: String, replyOnRecovery: Option[ActorRef[Any]] = None) =
    EventSourcedBehavior[TestCommand, Evt, EmptyState](PersistenceId.ofUniqueId(pid), EmptyState(), (_, cmd) => {
      cmd match {
        case Cmd(data) => Effect.persist(Evt(data))
        case Passivate => Effect.stop().thenRun(_ => replyOnRecovery.foreach(_ ! Stopped))
      }
    }, (_, _) => EmptyState()).snapshotWhen((_, _, _) => true).receiveSignal {
      case (_, RecoveryCompleted) => replyOnRecovery.foreach(_ ! Recovered)
    }

  def eventSourcedBehaviorWithState(pid: String, replyOnRecovery: Option[ActorRef[Any]] = None) =
    EventSourcedBehavior[TestCommand, Evt, NonEmptyState](
      PersistenceId.ofUniqueId(pid),
      NonEmptyState(""),
      (_, cmd) => {
        cmd match {
          case Cmd(data) => Effect.persist(Evt(data))
          case Passivate => Effect.stop().thenRun(_ => replyOnRecovery.foreach(_ ! Stopped))
        }
      },
      (state: NonEmptyState, event: Evt) => NonEmptyState(s"${state.data}${event.data}")).receiveSignal {
      case (_, RecoveryCompleted) => replyOnRecovery.foreach(_ ! Recovered)
    }

}
