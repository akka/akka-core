/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.annotation.{ DoNotInherit, InternalApi }

@DoNotInherit
sealed trait CompletionStrategy

case object CompletionStrategy {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] case object Immediately extends CompletionStrategy

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] case object Draining extends CompletionStrategy

  /**
   * The completion will be signaled immediately even if elements are still buffered.
   */
  def immediately: CompletionStrategy = Immediately

  /**
   * Already buffered elements will be signaled before signaling completion.
   */
  def draining: CompletionStrategy = Draining
}
