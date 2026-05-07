/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import java.util
import java.util.Comparator

import org.agrona.collections.Hashing
import org.agrona.collections.Object2IntHashMap

import akka.util.HashCode

/**
 * INTERNAL API: Versioned compression table to be advertised between systems
 *
 * @param version Either -1 for disabled or a version between 0 and 127
 */
private[remote] final class CompressionTable[T](
    val originUid: Long,
    val version: Byte,
    private val _dictionary: Object2IntHashMap[T]) {

  def dictionary: Map[T, Int] = {
    import scala.jdk.CollectionConverters._
    _dictionary.entrySet().iterator().asScala.map(entry => entry.getKey -> entry.getValue.intValue()).toMap
  }

  def compress(value: T): Int =
    _dictionary.getValue(value)

  def invert: DecompressionTable[T] =
    if (_dictionary.isEmpty) DecompressionTable.empty[T].copy(originUid = originUid, version = version)
    else {
      // TODO: these are some expensive checks, about the numbers being consecutive, without gaps
      // TODO: we can remove them, make them re-map (not needed I believe though)
      val expectedGaplessSum = Integer.valueOf((_dictionary.size * (_dictionary.size + 1)) / 2) /* Dirichlet */
      import scala.jdk.CollectionConverters._
      require(
        _dictionary.values.iterator().asScala.min == 0,
        "Compression table should start allocating from 0, yet lowest allocated id was " + _dictionary.values
          .iterator()
          .asScala
          .min)
      require(
        _dictionary.values.iterator().asScala.map(_.intValue()).sum + _dictionary.size == expectedGaplessSum,
        "Given compression map does not seem to be gap-less and starting from zero, " +
        "which makes compressing it into an Array difficult, bailing out! Map was: " + _dictionary)

      val tups = new Array[(Object, Int)](_dictionary.size).asInstanceOf[Array[(T, Int)]]
      val ts = new Array[Object](_dictionary.size).asInstanceOf[Array[T]]

      var i = 0
      val mit = _dictionary.entrySet().iterator
      while (i < tups.length) {
        val entry = mit.next()
        tups(i) = (entry.getKey -> entry.getValue.intValue())
        i += 1
      }
      util.Arrays.sort(tups, CompressionTable.compareBy2ndValue[T])

      i = 0
      while (i < tups.length) {
        ts(i) = tups(i)._1
        i += 1
      }

      DecompressionTable[T](originUid, version, ts)
    }

  override def toString: String = s"CompressionTable($originUid,$version,$dictionary)"

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, originUid)
    result = HashCode.hash(result, version)
    result = HashCode.hash(result, _dictionary)
    result
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: CompressionTable[_] =>
      originUid == other.originUid && version == other.version && _dictionary == other._dictionary
    case _ => false
  }
}

/** INTERNAL API */
private[remote] object CompressionTable {
  final val NotCompressedId = -1

  final val CompareBy2ndValue: Comparator[(Object, Int)] = new Comparator[(Object, Int)] {
    override def compare(o1: (Object, Int), o2: (Object, Int)): Int =
      o1._2.compare(o2._2)
  }
  def compareBy2ndValue[T]: Comparator[Tuple2[T, Int]] = CompareBy2ndValue.asInstanceOf[Comparator[(T, Int)]]

  private def newObject2IntHashMap[T](initialCapacity: Int): Object2IntHashMap[T] = {
    // import to use shouldAvoidAllocation = false because of concurrent access of dictionary
    new Object2IntHashMap[T](initialCapacity, Hashing.DEFAULT_LOAD_FACTOR, NotCompressedId, false)
  }

  def empty[T] = new CompressionTable[T](0, 0, newObject2IntHashMap(2))

  def apply[T](originUid: Long, version: Byte, dictionary: Map[T, Int]): CompressionTable[T] = {
    val _dictionary = newObject2IntHashMap[T](dictionary.size * 2)
    dictionary.foreach {
      case (key, value) => _dictionary.put(key, value)
    }
    new CompressionTable[T](originUid, version, _dictionary)
  }
}
