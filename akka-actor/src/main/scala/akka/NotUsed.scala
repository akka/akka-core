/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

/**
 * This type is used in generic type signatures wherever the actual value is of no importance.
 * It is a combination of Scala’s `Unit` and Java’s `Void`, which both have different issues when
 * used from the other language. An example use-case is the materialized value of an Akka Stream for cases
 * where no result shall be returned from materialization.
 */
sealed abstract class NotUsed

case object NotUsed extends NotUsed {

  /**
   * Java API: the singleton instance
   */
  def getInstance(): NotUsed = this

  /**
   * Java API: the singleton instance
   *
   * This is equivalent to [[NotUsed.getInstance]], but can be used with static import.
   */
  def notUsed(): NotUsed = this
}
