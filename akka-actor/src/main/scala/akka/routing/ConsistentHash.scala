/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.Arrays

import scala.collection.immutable
import scala.reflect.ClassTag

/**
 * Consistent Hashing node ring implementation.
 *
 * A good explanation of Consistent Hashing:
 * https://tom-e-white.com/2007/11/consistent-hashing.html
 *
 * Note that toString of the ring nodes are used for the node
 * hash, i.e. make sure it is different for different nodes.
 *
 */
class ConsistentHash[T: ClassTag] private (nodes: immutable.SortedMap[Int, T], val virtualNodesFactor: Int) {

  import ConsistentHash._

  if (virtualNodesFactor < 1) throw new IllegalArgumentException("virtualNodesFactor must be >= 1")

  // arrays for fast binary search and access
  // nodeHashRing is the sorted hash values of the nodes
  // nodeRing is the nodes sorted in the same order as nodeHashRing, i.e. same index
  private val (nodeHashRing: Array[Int], nodeRing: Array[T]) = {
    val (nhr: Seq[Int], nr: Seq[T]) = nodes.toSeq.unzip
    (nhr.toArray, nr.toArray)
  }

  /**
   * Adds a node to the node ring.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def :+(node: T): ConsistentHash[T] = {
    val nodeHash = hashFor(node.toString)
    new ConsistentHash(nodes ++ ((1 to virtualNodesFactor).map { r =>
      (concatenateNodeHash(nodeHash, r) -> node)
    }), virtualNodesFactor)
  }

  /**
   * Java API: Adds a node to the node ring.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def add(node: T): ConsistentHash[T] = this :+ node

  /**
   * Removes a node from the node ring.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def :-(node: T): ConsistentHash[T] = {
    val nodeHash = hashFor(node.toString)
    new ConsistentHash(nodes -- ((1 to virtualNodesFactor).map { r =>
      concatenateNodeHash(nodeHash, r)
    }), virtualNodesFactor)
  }

  /**
   * Java API: Removes a node from the node ring.
   * Note that the instance is immutable and this
   * operation returns a new instance.
   */
  def remove(node: T): ConsistentHash[T] = this :- node

  // converts the result of Arrays.binarySearch into a index in the nodeRing array
  // see documentation of Arrays.binarySearch for what it returns
  private def idx(i: Int): Int = {
    if (i >= 0) i // exact match
    else {
      val j = math.abs(i + 1)
      if (j >= nodeHashRing.length) 0 // after last, use first
      else j // next node clockwise
    }
  }

  /**
   * Get the node responsible for the data key.
   * Can only be used if nodes exists in the node ring,
   * otherwise throws `IllegalStateException`
   */
  def nodeFor(key: Array[Byte]): T = {
    if (isEmpty) throw new IllegalStateException("Can't get node for [%s] from an empty node ring".format(key))

    nodeRing(idx(Arrays.binarySearch(nodeHashRing, hashFor(key))))
  }

  /**
   * Get the node responsible for the data key.
   * Can only be used if nodes exists in the node ring,
   * otherwise throws `IllegalStateException`
   */
  def nodeFor(key: String): T = {
    if (isEmpty) throw new IllegalStateException("Can't get node for [%s] from an empty node ring".format(key))

    nodeRing(idx(Arrays.binarySearch(nodeHashRing, hashFor(key))))
  }

  /**
   * Is the node ring empty, i.e. no nodes added or all removed.
   */
  def isEmpty: Boolean = nodes.isEmpty

}

object ConsistentHash {
  def apply[T: ClassTag](nodes: Iterable[T], virtualNodesFactor: Int): ConsistentHash[T] = {
    new ConsistentHash(
      immutable.SortedMap.empty[Int, T] ++
      (for {
        node <- nodes
        nodeHash = hashFor(node.toString)
        vnode <- 1 to virtualNodesFactor
      } yield (concatenateNodeHash(nodeHash, vnode) -> node)),
      virtualNodesFactor)
  }

  /**
   * Java API: Factory method to create a ConsistentHash
   */
  def create[T](nodes: java.lang.Iterable[T], virtualNodesFactor: Int): ConsistentHash[T] = {
    import scala.jdk.CollectionConverters._
    apply(nodes.asScala, virtualNodesFactor)(ClassTag(classOf[Any].asInstanceOf[Class[T]]))
  }

  private def concatenateNodeHash(nodeHash: Int, vnode: Int): Int = {
    import MurmurHash._
    var h = startHash(nodeHash)
    h = extendHash(h, vnode, startMagicA, startMagicB)
    finalizeHash(h)
  }

  private def hashFor(bytes: Array[Byte]): Int = MurmurHash.arrayHash(bytes)

  private def hashFor(string: String): Int = MurmurHash.stringHash(string)
}
