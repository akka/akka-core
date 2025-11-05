/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.ActorRefProvider
import akka.actor.typed.RecipientRef
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait InternalRecipientRef[-T] extends RecipientRef[T] {

  /**
   * Get a reference to the actor ref provider which created this ref.
   */
  def provider: ActorRefProvider

  /**
   * @return `true` if the actor is locally known to be terminated, `false` if alive or uncertain.
   */
  def isTerminated: Boolean

  def refPrefix: String = toString

}
