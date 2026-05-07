/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.function

import scala.annotation.nowarn

/**
 * A Function interface. Used to create first-class-functions is Java.
 * `Serializable` is needed to be able to grab line number for Java 8 lambdas.
 * Supports throwing `Exception` in the apply, which the `java.util.function.Function` counterpart does not.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Function[-T, +R] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(param: T): R
}

/**
 * A Function interface. Used to create 2-arg first-class-functions is Java.
 * `Serializable` is needed to be able to grab line number for Java 8 lambdas.
 * Supports throwing `Exception` in the apply, which the `java.util.function.BiFunction` counterpart does not.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Function2[-T1, -T2, +R] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(arg1: T1, arg2: T2): R
}

/**
 * A Procedure is like a Function, but it doesn't produce a return value.
 * `Serializable` is needed to be able to grab line number for Java 8 lambdas.
 * Supports throwing `Exception` in the apply, which the `java.util.function.Consumer` counterpart does not.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Procedure[-T] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(param: T): Unit
}

/**
 * An executable piece of code that takes no parameters and doesn't return any value.
 * `Serializable` is needed to be able to grab line number for Java 8 lambdas.
 * Supports throwing `Exception` in the apply, which the `java.util.function.Effect` counterpart does not.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Effect extends java.io.Serializable {

  @throws(classOf[Exception])
  def apply(): Unit
}

/**
 * Java API: Defines a criteria and determines whether the parameter meets this criteria.
 * `Serializable` is needed to be able to grab line number for Java 8 lambdas.
 * Supports throwing `Exception` in the apply, which the `java.util.function.Predicate` counterpart does not.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Predicate[-T] extends java.io.Serializable {
  def test(param: T): Boolean
}

/**
 * A constructor/factory, takes no parameters but creates a new value of type T every call.
 * Supports throwing `Exception` in the apply, which the `java.util.function.Creator` counterpart does not.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Creator[+T] extends Serializable {

  /**
   * This method must return a different instance upon every call.
   */
  @throws(classOf[Exception])
  def create(): T
}
