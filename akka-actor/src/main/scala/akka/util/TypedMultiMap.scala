/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

/**
 * An immutable multi-map that expresses the value type as a type function of the key
 * type. Create it with a type constructor that expresses the relationship:
 *
 * {{{
 * trait Key { type Type = T }
 * case class MyValue[T](...)
 *
 * // type function from Key to MyValue
 * type KV[K <: Key] = MyValue[K#Type]
 *
 * val map = TypedMultiMap.empty[Key, KV]
 *
 * // a plain Int => String map would use this function:
 * type plain[K <: Int] = String
 *
 * // a map T => T would use this function:
 * type identity[T <: AnyRef] = T
 * }}}
 *
 * Caveat: using keys which take type parameters does not work due to conflicts
 * with the existential interpretation of `Key[_]`. A workaround is to define
 * a key type like above and provide a subtype that provides its type parameter
 * as type member `Type`.
 */
class TypedMultiMap[T <: AnyRef, K[_ <: T]] private (private val map: Map[T, Set[Any]]) {

  /**
   * Return the set of keys which are mapped to non-empty value sets.
   */
  def keySet: Set[T] = map.keySet

  /**
   * Return a map that has the given value added to the mappings for the given key.
   */
  def inserted(key: T)(value: K[key.type]): TypedMultiMap[T, K] = {
    val set = map.get(key) match {
      case Some(s) => s
      case None    => Set.empty[Any]
    }
    new TypedMultiMap[T, K](map.updated(key, set + value))
  }

  /**
   * Obtain all mappings for the given key.
   */
  def get(key: T): Set[K[key.type]] =
    map.get(key) match {
      case Some(s) => s.asInstanceOf[Set[K[key.type]]]
      case None    => Set.empty
    }

  /**
   * Return a map that has the given value removed from all keys.
   */
  def valueRemoved(value: Any): TypedMultiMap[T, K] = {
    val s = Set(value)
    val m = map.collect {
      case (k, set) if set != s => (k, set - value)
    }
    new TypedMultiMap[T, K](m)
  }

  /**
   * Return a map that has all mappings for the given key removed.
   */
  def keyRemoved(key: T): TypedMultiMap[T, K] = new TypedMultiMap[T, K](map - key)

  /**
   * Return a map that has the given mapping from the given key removed.
   */
  def removed(key: T)(value: K[key.type]): TypedMultiMap[T, K] = {
    map.get(key) match {
      case None => this
      case Some(set) =>
        if (set(value)) {
          val newset = set - value
          val newmap = if (newset.isEmpty) map - key else map.updated(key, newset)
          new TypedMultiMap[T, K](newmap)
        } else this
    }
  }

  def setAll(key: T)(values: Set[K[key.type]]): TypedMultiMap[T, K] =
    new TypedMultiMap[T, K](map.updated(key, values.asInstanceOf[Set[Any]]))

  /**
   * Add all entries from the other map, overwriting existing entries.
   *
   * FIXME: should it merge, instead?
   */
  def ++(other: TypedMultiMap[T, K]): TypedMultiMap[T, K] =
    new TypedMultiMap[T, K](map ++ other.map)

  override def toString: String = s"TypedMultiMap($map)"
  override def equals(other: Any) = other match {
    case o: TypedMultiMap[_, _] => map == o.map
    case _                      => false
  }
  override def hashCode: Int = map.hashCode
}

object TypedMultiMap {
  private val _empty = new TypedMultiMap[Nothing, Nothing](Map.empty)

  /**
   * Obtain the empty map for the given key type and key–value type function.
   */
  def empty[T <: AnyRef, K[_ <: T]]: TypedMultiMap[T, K] = _empty.asInstanceOf[TypedMultiMap[T, K]]
}
