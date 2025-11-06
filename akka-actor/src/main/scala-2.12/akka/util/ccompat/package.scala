/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://akka.io>
 */

package akka.util

import scala.{ collection => c }
import scala.collection.{ GenTraversable, immutable => i, mutable => m }
import scala.collection.generic.{ CanBuildFrom, GenericCompanion, Sorted, SortedSetFactory }
import scala.language.higherKinds
import scala.language.implicitConversions

/**
 * INTERNAL API
 *
 * Based on https://github.com/scala/scala-collection-compat/blob/master/compat/src/main/scala-2.11_2.12/scala/collection/compat/PackageShared.scala
 * but reproduced here so we don't need to add a dependency on this library. It contains much more than we need right now, and is
 * not promising binary compatibility yet at the time of writing.
 */
package object ccompat {
  import CompatImpl._

  /**
   * A factory that builds a collection of type `C` with elements of type `A`.
   *
   * @tparam A Type of elements (e.g. `Int`, `Boolean`, etc.)
   * @tparam C Type of collection (e.g. `List[Int]`, `TreeMap[Int, String]`, etc.)
   */
  private[akka] type Factory[-A, +C] = CanBuildFrom[Nothing, A, C]

  private[akka] implicit class FactoryOps[-A, +C](private val factory: Factory[A, C]) {

    /**
     * @return A collection of type `C` containing the same elements
     *         as the source collection `it`.
     * @param it Source collection
     */
    def fromSpecific(it: TraversableOnce[A]): C = (factory() ++= it).result()

    /**
     * Get a Builder for the collection. For non-strict collection types this will use an intermediate buffer.
     * Building collections with `fromSpecific` is preferred because it can be lazy for lazy collections.
     */
    def newBuilder: m.Builder[A, C] = factory()
  }

  private[akka] implicit def genericCompanionToCBF[A, CC[X] <: GenTraversable[X]](
      fact: GenericCompanion[CC]): CanBuildFrom[Any, A, CC[A]] =
    simpleCBF(fact.newBuilder[A])

  private[akka] implicit def sortedSetCompanionToCBF[
      A: Ordering,
      CC[X] <: c.SortedSet[X] with c.SortedSetLike[X, CC[X]]](fact: SortedSetFactory[CC]): CanBuildFrom[Any, A, CC[A]] =
    simpleCBF(fact.newBuilder[A])

  private[ccompat] def build[T, CC](builder: m.Builder[T, CC], source: TraversableOnce[T]): CC = {
    builder ++= source
    builder.result()
  }

  private[akka] implicit class ImmutableSortedMapExtensions(private val fact: i.SortedMap.type) extends AnyVal {
    def from[K: Ordering, V](source: TraversableOnce[(K, V)]): i.SortedMap[K, V] =
      build(i.SortedMap.newBuilder[K, V], source)
  }

  private[akka] implicit class ImmutableTreeMapExtensions(private val fact: i.TreeMap.type) extends AnyVal {
    def from[K: Ordering, V](source: TraversableOnce[(K, V)]): i.TreeMap[K, V] =
      build(i.TreeMap.newBuilder[K, V], source)
  }

  private[akka] implicit class SortedExtensionMethods[K, T <: Sorted[K, T]](private val fact: Sorted[K, T]) {
    def rangeFrom(from: K): T = fact.from(from)
    def rangeTo(to: K): T = fact.to(to)
    def rangeUntil(until: K): T = fact.until(until)
  }

  // This really belongs into scala.collection but there's already a package object
  // in scala-library so we can't add to it
  type IterableOnce[+X] = c.TraversableOnce[X]
  val IterableOnce = c.TraversableOnce

  implicit class ImmutableSortedSetOps[A](val real: i.SortedSet[A]) extends AnyVal {
    def unsorted: i.Set[A] = real
  }

  object JavaConverters extends scala.collection.convert.DecorateAsJava with scala.collection.convert.DecorateAsScala

  implicit def toTraversableOnceExtensionMethods[A](self: TraversableOnce[A]): TraversableOnceExtensionMethods[A] =
    new TraversableOnceExtensionMethods[A](self)
}

class TraversableOnceExtensionMethods[A](private val self: c.TraversableOnce[A]) extends AnyVal {
  def iterator: Iterator[A] = self.toIterator
}
