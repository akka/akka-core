/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import akka.annotation.InternalApi
import akka.japi.{ Pair => JPair }
import akka.japi.function.{ Function => JFun, Function2 => JFun2 }

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ConstantFun {

  private[this] val JavaIdentityFunction = new JFun[Any, Any] {
    @throws(classOf[Exception]) override def apply(param: Any): Any = param
  }

  val JavaPairFunction = new JFun2[AnyRef, AnyRef, AnyRef JPair AnyRef] {
    def apply(p1: AnyRef, p2: AnyRef): AnyRef JPair AnyRef = JPair(p1, p2)
  }

  def javaCreatePairFunction[A, B]: JFun2[A, B, JPair[A, B]] = JavaPairFunction.asInstanceOf[JFun2[A, B, JPair[A, B]]]

  def javaIdentityFunction[T]: JFun[T, T] = JavaIdentityFunction.asInstanceOf[JFun[T, T]]

  def scalaIdentityFunction[T]: T => T = conforms.asInstanceOf[Function[T, T]]

  def scalaAnyToNone[A, B]: A => Option[B] = none
  def scalaAnyToUnit[A]: A => Unit = unit
  def scalaAnyTwoToNone[A, B, C]: (A, B) => Option[C] = two2none
  def scalaAnyTwoToUnit[A, B]: (A, B) => Unit = two2unit
  def scalaAnyThreeToUnit[A, B, C]: (A, B, C) => Unit = three2unit
  def scalaAnyTwoToTrue[A, B]: (A, B) => Boolean = two2true
  def scalaAnyThreeToFalse[A, B, C]: (A, B, C) => Boolean = three2false
  def scalaAnyThreeToThird[A, B, C]: (A, B, C) => C = three2third.asInstanceOf[(A, B, C) => C]

  def javaAnyToNone[A, B]: A => Option[B] = none
  def nullFun[T] = _nullFun.asInstanceOf[Any => T]

  val zeroLong = (_: Any) => 0L

  val oneLong = (_: Any) => 1L

  val oneInt = (_: Any) => 1

  val unitToUnit = () => ()

  val anyToTrue: Any => Boolean = (_: Any) => true
  val anyToFalse: Any => Boolean = (_: Any) => false

  private val _nullFun = (_: Any) => null

  private val conforms = (a: Any) => a

  private val unit = (_: Any) => ()

  private val none = (_: Any) => None

  private val two2none = (_: Any, _: Any) => None

  private val two2true = (_: Any, _: Any) => true

  private val two2unit = (_: Any, _: Any) => ()

  private val three2unit = (_: Any, _: Any, _: Any) => ()

  private val three2false = (_: Any, _: Any, _: Any) => false

  private val three2third = (_: Any, _: Any, third: Any) => third

}
