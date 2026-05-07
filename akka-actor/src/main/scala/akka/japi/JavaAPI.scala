/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi

import java.util.Collections.{ emptyList, singletonList }

import scala.annotation.nowarn
import scala.collection.immutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.runtime.AbstractPartialFunction
import scala.util.control.NoStackTrace

import akka.util.Collections.EmptyImmutableSeq

/**
 * A Function interface. Used to create first-class-functions is Java.
 *
 * This class is kept for compatibility, but for future API's please prefer [[akka.japi.function.Function]].
 */
@FunctionalInterface
trait Function[T, R] {
  @throws(classOf[Exception])
  def apply(param: T): R
}

/**
 * A Function interface. Used to create 2-arg first-class-functions is Java.
 *
 * This class is kept for compatibility, but for future API's please prefer [[akka.japi.function.Function2]].
 */
@FunctionalInterface
trait Function2[T1, T2, R] {
  @throws(classOf[Exception])
  def apply(arg1: T1, arg2: T2): R
}

/**
 * A Procedure is like a Function, but it doesn't produce a return value.
 *
 * This class is kept for compatibility, but for future API's please prefer [[akka.japi.function.Procedure]].
 */
@FunctionalInterface
trait Procedure[T] {
  @throws(classOf[Exception])
  def apply(param: T): Unit
}

/**
 * An executable piece of code that takes no parameters and doesn't return any value.
 *
 * This class is kept for compatibility, but for future API's please prefer [[akka.japi.function.Effect]].
 */
@FunctionalInterface
trait Effect {
  @throws(classOf[Exception])
  def apply(): Unit
}

/**
 * Java API: Defines a criteria and determines whether the parameter meets this criteria.
 *
 * This class is kept for compatibility, but for future API's please prefer [[java.util.function.Predicate]].
 */
@FunctionalInterface
trait Predicate[T] {
  def test(param: T): Boolean
}

/**
 * Java API
 * Represents a pair (tuple) of two elements.
 *
 * Additional tuple types for 3 to 22 values are defined in the `akka.japi.tuple` package, e.g. [[akka.japi.tuple.Tuple3]].
 */
@SerialVersionUID(1L)
case class Pair[A, B](first: A, second: B) {
  def toScala: (A, B) = (first, second)
}
object Pair {
  def create[A, B](first: A, second: B): Pair[A, B] = new Pair(first, second)
}

/**
 * A constructor/factory, takes no parameters but creates a new value of type T every call.
 *
 * This class is kept for compatibility, but for future API's please prefer [[akka.japi.function.Creator]].
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
@FunctionalInterface
trait Creator[T] extends Serializable {

  /**
   * This method must return a different instance upon every call.
   */
  @throws(classOf[Exception])
  def create(): T
}

object JavaPartialFunction {
  sealed abstract class NoMatchException extends RuntimeException with NoStackTrace
  case object NoMatch extends NoMatchException
  final def noMatch(): RuntimeException = NoMatch
}

/**
 * Helper for implementing a *pure* partial function: it will possibly be
 * invoked multiple times for a single “application”, because its only abstract
 * method is used for both isDefinedAt() and apply(); the former is mapped to
 * `isCheck == true` and the latter to `isCheck == false` for those cases where
 * this is important to know.
 *
 * Failure to match is signaled by throwing `noMatch()`, i.e. not returning
 * normally (the exception used in this case is pre-allocated, hence not
 * <i>that</i> expensive).
 *
 * {{{
 * new JavaPartialFunction<Object, String>() {
 *   public String apply(Object in, boolean isCheck) {
 *     if (in instanceof TheThing) {
 *       if (isCheck) return null; // to spare the expensive or side-effecting code
 *       return doSomethingWithTheThing((TheThing) in);
 *     } else {
 *       throw noMatch();
 *     }
 *   }
 * }
 * }}}
 *
 * The typical use of partial functions from Akka looks like the following:
 *
 * {{{
 * if (pf.isDefinedAt(x)) {
 *   pf.apply(x);
 * }
 * }}}
 *
 * i.e. it will first call `JavaPartialFunction.apply(x, true)` and if that
 * does not throw `noMatch()` it will continue with calling
 * `JavaPartialFunction.apply(x, false)`.
 */
abstract class JavaPartialFunction[A, B] extends AbstractPartialFunction[A, B] {
  import JavaPartialFunction._

  @throws(classOf[Exception])
  def apply(x: A, isCheck: Boolean): B

  final def isDefinedAt(x: A): Boolean =
    try {
      apply(x, isCheck = true); true
    } catch { case NoMatch => false }
  final override def apply(x: A): B =
    try apply(x, isCheck = false)
    catch { case NoMatch => throw new MatchError(x) }
  final override def applyOrElse[A1 <: A, B1 >: B](x: A1, default: A1 => B1): B1 =
    try apply(x, isCheck = false)
    catch { case NoMatch => default(x) }
}

/**
 * This class represents optional values. Instances of <code>Option</code>
 * are either instances of case class <code>Some</code> or it is case
 * object <code>None</code>.
 */
sealed abstract class Option[A] extends java.lang.Iterable[A] {
  def get: A

  /**
   * Returns <code>a</code> if this is <code>some(a)</code> or <code>defaultValue</code> if
   * this is <code>none</code>.
   */
  def getOrElse[B >: A](defaultValue: B): B
  def isEmpty: Boolean
  def isDefined: Boolean = !isEmpty
  def asScala: scala.Option[A]
  def iterator: java.util.Iterator[A] = if (isEmpty) emptyList[A].iterator else singletonList(get).iterator
}

object Option {

  /**
   * <code>Option</code> factory that creates <code>Some</code>
   */
  def some[A](v: A): Option[A] = Some(v)

  /**
   * <code>Option</code> factory that creates <code>None</code>
   */
  def none[A]: Option[A] = None.asInstanceOf[Option[A]]

  /**
   * <code>Option</code> factory that creates <code>None</code> if
   * <code>v</code> is <code>null</code>, <code>Some(v)</code> otherwise.
   */
  def option[A](v: A): Option[A] = if (v == null) none else some(v)

  /**
   * Converts a Scala Option to a Java Option
   */
  def fromScalaOption[T](scalaOption: scala.Option[T]): Option[T] = scalaOption match {
    case scala.Some(r) => some(r)
    case scala.None    => none
  }

  /**
   * Class <code>Some[A]</code> represents existing values of type
   * <code>A</code>.
   */
  final case class Some[A](v: A) extends Option[A] {
    def get: A = v
    def getOrElse[B >: A](defaultValue: B): B = v
    def isEmpty: Boolean = false
    def asScala: scala.Some[A] = scala.Some(v)
  }

  /**
   * This case object represents non-existent values.
   */
  private case object None extends Option[Nothing] {
    def get: Nothing = throw new NoSuchElementException("None.get")
    def getOrElse[B](defaultValue: B): B = defaultValue
    def isEmpty: Boolean = true
    def asScala: scala.None.type = scala.None
  }

  implicit def java2ScalaOption[A](o: Option[A]): scala.Option[A] = o.asScala
  implicit def scala2JavaOption[A](o: scala.Option[A]): Option[A] = if (o.isDefined) some(o.get) else none
}

/**
 * This class hold common utilities for Java
 */
object Util {

  /**
   * Returns a ClassTag describing the provided Class.
   */
  def classTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /**
   * Returns an immutable.Seq representing the provided array of Classes,
   * an overloading of the generic immutableSeq in Util, to accommodate for erasure.
   */
  def immutableSeq(arr: Array[Class[_]]): immutable.Seq[Class[_]] = immutableSeq[Class[_]](arr)

  /**
   * Turns an array into an immutable Scala sequence (by copying it).
   */
  def immutableSeq[T](arr: Array[T]): immutable.Seq[T] =
    if ((arr ne null) && arr.length > 0) arr.toIndexedSeq else Nil

  /**
   * Turns an [[java.lang.Iterable]] into an immutable Scala sequence (by copying it).
   */
  def immutableSeq[T](iterable: java.lang.Iterable[T]): immutable.Seq[T] =
    iterable match {
      case imm: immutable.Seq[_] => imm.asInstanceOf[immutable.Seq[T]]
      case other =>
        val i = other.iterator()
        if (i.hasNext) {
          val builder = new immutable.VectorBuilder[T]

          while ({ builder += i.next(); i.hasNext }) ()

          builder.result()
        } else EmptyImmutableSeq
    }

  def immutableSingletonSeq[T](value: T): immutable.Seq[T] = value :: Nil

  def javaArrayList[T](seq: Seq[T]): java.util.List[T] = {
    val size = seq.size
    val l = new java.util.ArrayList[T](size)
    seq.foreach(l.add) // TODO could be optimised based on type of Seq
    l
  }

  /**
   * Turns an [[java.lang.Iterable]] into an immutable Scala IndexedSeq (by copying it).
   */
  def immutableIndexedSeq[T](iterable: java.lang.Iterable[T]): immutable.IndexedSeq[T] =
    immutableSeq(iterable).toVector

  // TODO in case we decide to pull in scala-java8-compat methods below could be removed - https://github.com/akka/akka-core/issues/16247

  def option[T](jOption: java.util.Optional[T]): scala.Option[T] =
    scala.Option(jOption.orElse(null.asInstanceOf[T]))
}
