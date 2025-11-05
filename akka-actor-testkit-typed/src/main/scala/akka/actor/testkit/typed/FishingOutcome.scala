/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed

import akka.annotation.DoNotInherit

/**
 * Not for user extension.
 *
 * Instances are available from `FishingOutcomes` in the respective dsls: [[akka.actor.testkit.typed.scaladsl.FishingOutcomes]]
 * and [[akka.actor.testkit.typed.javadsl.FishingOutcomes]]
 */
@DoNotInherit sealed trait FishingOutcome

object FishingOutcome {

  sealed trait ContinueOutcome extends FishingOutcome
  case object Continue extends ContinueOutcome
  case object ContinueAndIgnore extends ContinueOutcome
  case object Complete extends FishingOutcome
  final case class Fail(error: String) extends FishingOutcome
}
