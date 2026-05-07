/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl

import akka.actor.typed.BackoffSupervisorStrategy
import akka.actor.typed.Behavior
import akka.actor.typed.TypedActorContext
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.typed.internal.ReplicationContextImpl

import java.util.Optional
import scala.jdk.FutureConverters.CompletionStageOps

/**
 * Base class for replicated event sourced behaviors for projects built with Java 17 or newer where message handling
 * can be done using switch pattern match.
 *
 * Enforces replies to every received command.
 *
 * For building replicated event sourced actors with Java versions before 17, see [[ReplicatedEventSourcedBehavior]]
 */
abstract class ReplicatedEventSourcedOnCommandWithReplyBehavior[Command, Event, State](
    replicationContext: ReplicationContext,
    onPersistFailure: Optional[BackoffSupervisorStrategy])
    extends EventSourcedOnCommandWithReplyBehavior[Command, Event, State](
      replicationContext.persistenceId,
      onPersistFailure) {

  def this(replicationContext: ReplicationContext) = this(replicationContext, Optional.empty())

  /**
   * Override and return false to disable events being published to the system event stream as
   * [[akka.persistence.typed.PublishedEvent]] after they have been persisted.
   */
  def withEventPublishing: Boolean = true

  protected def getReplicationContext(): ReplicationContext = replicationContext

  /**
   * INTERNAL API: DeferredBehavior init, not for user extension
   */
  @InternalApi override def apply(context: TypedActorContext[Command]): Behavior[Command] = {
    createEventSourcedBehavior()
    // context not user extendable so there should never be any other impls
      .withReplication(replicationContext.asInstanceOf[ReplicationContextImpl])
      .withEventPublishing(withEventPublishing)
  }

  /** INTERNAL API */
  @InternalApi
  override private[akka] def createEventSourcedBehavior() = {
    var behavior = super.createEventSourcedBehavior()
    replicationInterceptor.ifPresent(ri =>
      behavior = behavior.withReplicatedEventInterceptor(ri.intercept(_, _, _, _).asScala))

    behavior
  }

  /**
   * If a callback is returned it is invoked when an event from another replica arrives, delaying persisting the event until the returned
   * completion stage completes, if the future fails the actor is crashed.
   *
   * Only used when the entity is replicated.
   */
  @ApiMayChange
  def replicationInterceptor: Optional[ReplicationInterceptor[Event, State]] = Optional.empty()
}
