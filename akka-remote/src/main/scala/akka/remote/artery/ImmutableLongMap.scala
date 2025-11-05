/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.Arrays

import scala.annotation.tailrec
import scala.reflect.ClassTag

import akka.util.HashCode
import akka.util.OptionVal

/**
 * INTERNAL API
 */
private[akka] object ImmutableLongMap {
  def empty[A >: Null](implicit t: ClassTag[A]): ImmutableLongMap[A] =
    new ImmutableLongMap(Array.emptyLongArray, Array.empty)
}

/**
 * INTERNAL API
 * Specialized Map for primitive `Long` keys to avoid allocations (boxing).
 * Keys and values are backed by arrays and lookup is performed with binary
 * search. It's intended for rather small (<1000) maps.
 */
private[akka] class ImmutableLongMap[A >: Null] private (private val keys: Array[Long], private val values: Array[A])(
    implicit t: ClassTag[A]) {

  val size: Int = keys.length

  /**
   * Worst case `O(log n)`, allocation free.
   */
  def get(key: Long): OptionVal[A] = {
    val i = Arrays.binarySearch(keys, key)
    if (i >= 0) OptionVal(values(i))
    else OptionVal.None
  }

  /**
   * Worst case `O(log n)`, allocation free.
   */
  def contains(key: Long): Boolean = {
    Arrays.binarySearch(keys, key) >= 0
  }

  /**
   * Worst case `O(log n)`, creates new `ImmutableLongMap`
   * with copies of the internal arrays for the keys and
   * values.
   */
  def updated(key: Long, value: A): ImmutableLongMap[A] = {
    if (size == 0)
      new ImmutableLongMap(Array(key), Array(value))
    else {
      val i = Arrays.binarySearch(keys, key)
      if (i >= 0) {
        // existing key, replace value
        val newValues = new Array[A](values.length)
        System.arraycopy(values, 0, newValues, 0, values.length)
        newValues(i) = value
        new ImmutableLongMap(keys, newValues)
      } else {
        // insert the entry at the right position, and keep the arrays sorted
        val j = -(i + 1)
        val newKeys = new Array[Long](size + 1)
        System.arraycopy(keys, 0, newKeys, 0, j)
        newKeys(j) = key
        System.arraycopy(keys, j, newKeys, j + 1, keys.length - j)

        val newValues = new Array[A](size + 1)
        System.arraycopy(values, 0, newValues, 0, j)
        newValues(j) = value
        System.arraycopy(values, j, newValues, j + 1, values.length - j)

        new ImmutableLongMap(newKeys, newValues)
      }
    }
  }

  def remove(key: Long): ImmutableLongMap[A] = {
    val i = Arrays.binarySearch(keys, key)
    if (i >= 0) {
      if (size == 1)
        ImmutableLongMap.empty
      else {
        val newKeys = new Array[Long](size - 1)
        System.arraycopy(keys, 0, newKeys, 0, i)
        System.arraycopy(keys, i + 1, newKeys, i, keys.length - i - 1)

        val newValues = new Array[A](size - 1)
        System.arraycopy(values, 0, newValues, 0, i)
        System.arraycopy(values, i + 1, newValues, i, values.length - i - 1)

        new ImmutableLongMap(newKeys, newValues)
      }
    } else
      this
  }

  /**
   * All keys
   */
  def keysIterator: Iterator[Long] =
    keys.iterator

  override def toString: String =
    keysIterator.map(key => s"$key -> ${get(key).get}").mkString("ImmutableLongMap(", ", ", ")")

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, keys)
    result = HashCode.hash(result, values)
    result
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: ImmutableLongMap[_] =>
      if (other eq this) true
      else if (size != other.size) false
      else if (size == 0 && other.size == 0) true
      else {
        @tailrec def check(i: Int): Boolean = {
          if (i == size) true
          else if (keys(i) == other.keys(i) && values(i) == other.values(i))
            check(i + 1) // recur, next elem
          else false
        }
        check(0)
      }
    case _ => false
  }
}
