/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.immutable

/**
 * Typeclass which describes a classification hierarchy. Observe the contract between `isEqual` and `isSubclass`!
 */
trait Subclassification[K] {

  /**
   * True if and only if x and y are of the same class.
   */
  def isEqual(x: K, y: K): Boolean

  /**
   * True if and only if x is a subclass of y; equal classes must be considered sub-classes!
   */
  def isSubclass(x: K, y: K): Boolean
}

private[akka] object SubclassifiedIndex {

  class Nonroot[K, V](override val root: SubclassifiedIndex[K, V], val key: K, _values: Set[V])(
      implicit sc: Subclassification[K])
      extends SubclassifiedIndex[K, V](_values) {

    override def innerAddValue(key: K, value: V): Changes = {
      // break the recursion on super when key is found and transition to recursive add-to-set
      if (sc.isEqual(key, this.key)) addValue(value) else super.innerAddValue(key, value)
    }

    private def addValue(value: V): Changes = {
      val kids = subkeys.flatMap(_.addValue(value))
      if (!(values contains value)) {
        values += value
        kids :+ ((key, Set(value)))
      } else kids
    }

    // this will return the keys and values to be removed from the cache
    override def innerRemoveValue(key: K, value: V): Changes = {
      // break the recursion on super when key is found and transition to recursive remove-from-set
      if (sc.isEqual(key, this.key)) removeValue(value) else super.innerRemoveValue(key, value)
    }

    override def removeValue(value: V): Changes = {
      val kids = subkeys.flatMap(_.removeValue(value))
      if (values contains value) {
        values -= value
        kids :+ ((key, Set(value)))
      } else kids
    }

    override def innerFindValues(key: K): Set[V] =
      if (sc.isEqual(key, this.key)) values else super.innerFindValues(key)

    override def toString = subkeys.mkString("Nonroot(" + key + ", " + values + ",\n", ",\n", ")")
  }

  private[SubclassifiedIndex] def emptyMergeMap[K, V] = internalEmptyMergeMap.asInstanceOf[Map[K, Set[V]]]
  private[this] val internalEmptyMergeMap = Map[AnyRef, Set[AnyRef]]().withDefaultValue(Set[AnyRef]())
}

/**
 * Mutable index which respects sub-class relationships between keys:
 *
 *  - adding a key inherits from super-class
 *  - adding values applies to all sub-classes
 *  - removing values applies to all sub-classes
 *
 * Currently this class is only used to keep the tree and return changed key-
 * value-sets upon modification, since looking up the keys in an external
 * cache, e.g. HashMap, is faster than tree traversal which must use linear
 * scan at each level. Therefore, no value traversals are published.
 */
private[akka] class SubclassifiedIndex[K, V] private (protected var values: Set[V])(implicit sc: Subclassification[K]) {

  import SubclassifiedIndex._

  type Changes = immutable.Seq[(K, Set[V])]

  protected var subkeys = Vector.empty[Nonroot[K, V]]

  def this()(implicit sc: Subclassification[K]) = this(Set.empty)

  protected val root = this

  /**
   * Add key to this index which inherits its value set from the most specific
   * super-class which is known.
   *
   * @return the diff that should be added to the cache
   */
  def addKey(key: K): Changes = mergeChangesByKey(innerAddKey(key))

  protected def innerAddKey(key: K): Changes = {
    var found = false
    val ch = subkeys.flatMap { n =>
      if (sc.isEqual(key, n.key)) {
        found = true
        Nil
      } else if (sc.isSubclass(key, n.key)) {
        found = true
        n.innerAddKey(key)
      } else Nil
    }
    if (!found) {
      integrate(new Nonroot(root, key, values)) :+ ((key, values))
    } else ch
  }

  /**
   * Add value to all keys which are subclasses of the given key. If the key
   * is not known yet, it is inserted as if using addKey.
   *
   * @return the diff that should be added to the cache
   */
  def addValue(key: K, value: V): Changes = mergeChangesByKey(innerAddValue(key, value))

  protected def innerAddValue(key: K, value: V): Changes = {
    var found = false
    val ch = subkeys.flatMap { n =>
      if (sc.isSubclass(key, n.key)) {
        found = true
        n.innerAddValue(key, value)
      } else Nil
    }
    if (!found) {
      val v = values + value
      val n = new Nonroot(root, key, v)
      integrate(n) ++ n.innerAddValue(key, value) :+ (key -> v)
    } else ch
  }

  /**
   * Remove value from all keys which are subclasses of the given key.
   *
   * @return the complete changes that should be updated in the cache
   */
  def removeValue(key: K, value: V): Changes =
    // the reason for not using the values in the returned diff is that we need to
    // go through the whole tree to find all values for the "changed" keys in other
    // parts of the tree as well, since new nodes might have been created
    mergeChangesByKey(innerRemoveValue(key, value)).map {
      case (k, _) => (k, findValues(k))
    }

  // this will return the keys and values to be removed from the cache
  protected def innerRemoveValue(key: K, value: V): Changes = {
    var found = false
    val ch = subkeys.flatMap { n =>
      if (sc.isSubclass(key, n.key)) {
        found = true
        n.innerRemoveValue(key, value)
      } else Nil
    }
    if (!found) {
      val n = new Nonroot(root, key, values)
      integrate(n) ++ n.removeValue(value)
    } else ch
  }

  /**
   * Remove value from all keys in the index.
   *
   * @return the diff that should be removed from the cache
   */
  def removeValue(value: V): Changes = mergeChangesByKey(subkeys.flatMap(_.removeValue(value)))

  /**
   * Find all values for a given key in the index.
   */
  protected final def findValues(key: K): Set[V] = root.innerFindValues(key)
  protected def innerFindValues(key: K): Set[V] =
    subkeys.foldLeft(Set.empty[V]) { (s, n) =>
      if (sc.isSubclass(key, n.key))
        s ++ n.innerFindValues(key)
      else
        s
    }

  /**
   * Find all subkeys of a given key in the index excluding some subkeys.
   */
  protected final def findSubKeysExcept(key: K, except: Vector[Nonroot[K, V]]): Set[K] =
    root.innerFindSubKeys(key, except)
  protected def innerFindSubKeys(key: K, except: Vector[Nonroot[K, V]]): Set[K] =
    subkeys.foldLeft(Set.empty[K]) { (s, n) =>
      if (sc.isEqual(key, n.key)) s
      else
        n.innerFindSubKeys(key, except) ++ {
          if (sc.isSubclass(n.key, key) && !except.exists(e => sc.isEqual(key, e.key)))
            s + n.key
          else
            s
        }
    }

  override def toString = subkeys.mkString("SubclassifiedIndex(" + values + ",\n", ",\n", ")")

  /**
   * Add new Nonroot below this node and check all existing nodes for subclass relationship.
   * Also needs to find subkeys in other parts of the tree to compensate for multiple inheritance.
   */
  private def integrate(n: Nonroot[K, V]): Changes = {
    val (subsub, sub) = subkeys.partition(k => sc.isSubclass(k.key, n.key))
    subkeys = sub :+ n
    n.subkeys = if (subsub.nonEmpty) subsub else n.subkeys
    n.subkeys ++= findSubKeysExcept(n.key, n.subkeys).map(k => new Nonroot(root, k, values))
    n.subkeys.map(n => (n.key, n.values.toSet))
  }

  private def mergeChangesByKey(changes: Changes): Changes =
    changes
      .foldLeft(emptyMergeMap[K, V]) {
        case (m, (k, s)) => m.updated(k, m(k) ++ s)
      }
      .to(immutable.Seq)
}
