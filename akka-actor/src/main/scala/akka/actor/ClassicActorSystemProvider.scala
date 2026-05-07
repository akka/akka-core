/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import akka.annotation.DoNotInherit
import akka.annotation.InternalApi

/**
 * Glue API introduced to allow minimal user effort integration between classic and typed for example for streams.
 *
 * Not for user extension.
 */
@DoNotInherit
trait ClassicActorSystemProvider {

  /**
   * Allows access to the classic `akka.actor.ActorSystem` even for `akka.actor.typed.ActorSystem[_]`s.
   */
  def classicSystem: ActorSystem
}

/**
 * Glue API introduced to allow minimal user effort integration between classic and typed for example for streams.
 *
 * Not for user extension.
 */
@DoNotInherit
trait ClassicActorContextProvider {

  /** INTERNAL API */
  @InternalApi
  private[akka] def classicActorContext: ActorContext
}
