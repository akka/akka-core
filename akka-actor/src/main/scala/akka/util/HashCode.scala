/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.lang.{ Float => JFloat, Double => JDouble }
import java.lang.reflect.{ Array => JArray }

/**
 * Set of methods which allow easy implementation of <code>hashCode</code>.
 *
 * Example:
 * <pre>
 *  override def hashCode: Int = {
 *    var result = HashCode.SEED
 *    //collect the contributions of various fields
 *    result = HashCode.hash(result, fPrimitive)
 *    result = HashCode.hash(result, fObject)
 *    result = HashCode.hash(result, fArray)
 *    result
 *  }
 * </pre>
 */
object HashCode {
  val SEED = 23

  def hash(seed: Int, any: Any): Int = any match {
    case value: Boolean => hash(seed, value)
    case value: Char    => hash(seed, value)
    case value: Short   => hash(seed, value)
    case value: Int     => hash(seed, value)
    case value: Long    => hash(seed, value)
    case value: Float   => hash(seed, value)
    case value: Double  => hash(seed, value)
    case value: Byte    => hash(seed, value)
    case value: AnyRef =>
      var result = seed
      if (value eq null) result = hash(result, 0)
      else if (!isArray(value)) result = hash(result, value.hashCode())
      else for (id <- 0 until JArray.getLength(value)) result = hash(result, JArray.get(value, id)) // is an array
      result
    case unexpected =>
      throw new IllegalArgumentException(s"Unexpected hash parameter: $unexpected") // will not happen, for exhaustiveness check
  }
  def hash(seed: Int, value: Boolean): Int = firstTerm(seed) + (if (value) 1 else 0)
  def hash(seed: Int, value: Char): Int = firstTerm(seed) + value.asInstanceOf[Int]
  def hash(seed: Int, value: Int): Int = firstTerm(seed) + value
  def hash(seed: Int, value: Long): Int = firstTerm(seed) + (value ^ (value >>> 32)).asInstanceOf[Int]
  def hash(seed: Int, value: Float): Int = hash(seed, JFloat.floatToIntBits(value))
  def hash(seed: Int, value: Double): Int = hash(seed, JDouble.doubleToLongBits(value))

  private def firstTerm(seed: Int): Int = PRIME * seed
  private def isArray(anyRef: AnyRef): Boolean = anyRef.getClass.isArray
  private val PRIME = 37
}
