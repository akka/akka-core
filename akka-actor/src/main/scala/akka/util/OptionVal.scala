/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import akka.annotation.InternalStableApi

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object OptionVal {

  def apply[A](x: A): OptionVal[A] = new OptionVal(x)

  object Some {
    def apply[A](x: A): OptionVal[A] = new OptionVal(x)
    def unapply[A](x: OptionVal[A]): OptionVal[A] = x
  }

  def none[A]: OptionVal[A] = None.asInstanceOf[OptionVal[A]]

  /**
   * Represents non-existent values, `null` values.
   */
  val None = new OptionVal[Null](null)
}

/**
 * INTERNAL API
 * Represents optional values similar to `scala.Option`, but
 * as a value class to avoid allocations.
 *
 * Note that it can be used in pattern matching without allocations
 * because it has name based extractor using methods `isEmpty` and `get`.
 * See https://hseeberger.wordpress.com/2013/10/04/name-based-extractors-in-scala-2-11/
 */
@InternalStableApi
private[akka] final class OptionVal[+A](val x: A) extends AnyVal {

  /**
   * Returns true if the option is `OptionVal.None`, false otherwise.
   */
  def isEmpty: Boolean =
    x == null

  /**
   * Returns false if the option is `OptionVal.None`, true otherwise.
   */
  def isDefined: Boolean = !isEmpty

  /**
   * Returns the option's value if the option is nonempty, otherwise
   * return `default`.
   */
  def getOrElse[B >: A](default: B): B =
    if (x == null) default else x

  /**
   * Convert to `scala.Option`
   */
  def toOption: Option[A] =
    Option(x)

  def contains[B >: A](it: B): Boolean =
    x != null && x == it

  /**
   *  Returns the option's value if it is nonempty, or `null` if it is empty.
   */
  def orNull[A1 >: A](implicit ev: Null <:< A1): A1 = this.getOrElse(ev(null))

  /**
   * Returns the option's value.
   * @note The option must be nonEmpty.
   * @throws java.util.NoSuchElementException if the option is empty.
   */
  def get: A =
    if (x == null) throw new NoSuchElementException("OptionVal.None.get")
    else x

  override def toString: String =
    if (x == null) "None" else s"Some($x)"
}
